package seglog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// log.go：分段追加日志的对外类型与生命周期管理。

// Log 是一个可崩溃恢复的分段式追加日志。多个 goroutine 可并发使用。
type Log struct {
	dir string
	cfg Config

	mu sync.Mutex

	closed bool

	// segments 为当前保留的分段，按 basePosition 升序；最后一个为 active。
	segments []*segment
	// active 为正在追加的尾部分段。
	active *segment
	// cache 管理只读分段句柄的有界缓存。
	cache *fileCache

	// firstPosition 当前仍可读取的最早位置（首个分段 basePosition）。
	firstPosition uint64
	// committedPosition 已提交位置（下一条记录序号）。
	committedPosition uint64
	// totalBytes 全部分段字节总数。
	totalBytes int64

	// readers 记录所有存活读取方，回收安全边界取其最小位置。
	readers map[*Reader]struct{}

	// notify 在已提交位置推进或日志关闭时广播。
	notify chan struct{}

	// appendMu 串行化所有写路径（mu 只保护内存状态，写盘不持锁）。
	appendMu sync.Mutex
}

// Open 打开（必要时创建）目录下的日志，先执行确定性恢复。
func Open(dir string, cfg Config) (*Log, *RecoveryInfo, error) {
	cfg = cfg.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	info, err := recoverDir(dir, cfg)
	if err != nil {
		return nil, nil, err
	}

	l := &Log{
		dir:               dir,
		cfg:               cfg,
		readers:           make(map[*Reader]struct{}),
		notify:            make(chan struct{}),
		firstPosition:     info.FirstPosition,
		committedPosition: info.CommittedPosition,
		cache:             newFileCache(cfg.MaxOpenFiles),
	}

	// 按恢复结论重新装载分段。
	for _, m := range info.Segments {
		fi, err := os.Stat(filepath.Join(dir, segmentName(m.BasePosition)))
		if err != nil {
			return nil, nil, err
		}
		l.segments = append(l.segments, &segment{
			path:         filepath.Join(dir, segmentName(m.BasePosition)),
			basePosition: m.BasePosition,
			size:         fi.Size(),
			startOffset:  l.totalBytes,
		})
		l.totalBytes += fi.Size()
	}
	if len(l.segments) == 0 {
		// 空目录：建立第一个活动分段（位置从 0 起）。
		seg, err := createSegment(dir, info.CommittedPosition, 0)
		if err != nil {
			return nil, nil, err
		}
		l.segments = append(l.segments, seg)
		l.firstPosition = info.CommittedPosition
	}
	l.active = l.segments[len(l.segments)-1]
	if err := l.active.openWrite(); err != nil {
		return nil, nil, err
	}

	return l, info, nil
}

// Append 追加一条记录并阻塞直到按配置要求落盘；返回该记录的逻辑位置
// （即其记录序号）。多帧拆分写入；全部帧写齐且 fsync 后才推进提交位置，
// 因此读取方永远看不到半条记录，崩溃后该记录整体消失。
func (l *Log) Append(ctx context.Context, data []byte) (uint64, error) {
	if l.cfg.MaxRecordBytes > 0 && len(data) > l.cfg.MaxRecordBytes {
		return 0, fmt.Errorf("%w: %d > max %d", ErrRecordTooLarge, len(data), l.cfg.MaxRecordBytes)
	}
	l.appendMu.Lock()
	defer l.appendMu.Unlock()

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return 0, ErrClosed
	}
	seq := l.committedPosition
	cfg := l.cfg
	l.mu.Unlock()

	if hook := cfg.beforeFrameSync; hook != nil {
		if err := hook(phaseBeforeWrite); err != nil {
			return 0, err
		}
	}

	// 在写锁保护下完成所有写盘帧，保证记录在文件内连续且顺序确定。
	pos, err := l.writeRecord(seq, data)
	if err != nil {
		return 0, err
	}

	if hook := cfg.beforeFrameSync; hook != nil {
		if err := hook(phaseBeforeFsync); err != nil {
			return 0, err
		}
	}
	if cfg.FsyncEveryWrite {
		l.mu.Lock()
		act := l.active
		l.mu.Unlock()
		if err := act.writeFile.Sync(); err != nil {
			return 0, err
		}
	}
	if hook := cfg.beforeFrameSync; hook != nil {
		if err := hook(phaseAfterFsync); err != nil {
			return 0, err
		}
	}

	// 提交：推进内存位置并广播。
	l.mu.Lock()
	l.committedPosition = seq + 1
	ch := l.notify
	l.mu.Unlock()
	l.broadcast(ch)
	return pos, nil
}

// writeRecord 把一条记录切成一到多个帧写入，必要时滚动分段。
// 返回记录位置。任何写入错误都使该记录不被提交（恢复时作为残余丢弃）。
//
// 不变量：一条记录的全部分片必须落在连续分段中，且滚动只允许发生在
// 记录的“第一个帧”之前——中间帧绝不允许成为新分段的第一帧，否则
// 恢复时该分段会缺失首帧而判定记录损坏。为此默认帧负载不超过
// MaxSegmentBytes；若单个帧（含极小记录的首帧）仍大于当前已用量，
// 则先滚动到空分段，空分段总是容纳得下一帧。
func (l *Log) writeRecord(seq uint64, data []byte) (uint64, error) {
	chunk := l.cfg.chunkSize()
	if int64(chunk) > l.cfg.MaxSegmentBytes {
		chunk = int(l.cfg.MaxSegmentBytes)
	}
	first := true
	var writeOff int64 // 当前活动分段内的写入偏移（pwrite，不依赖共享 fd 偏移）
	for offset := 0; offset < len(data) || first; {
		end := offset + chunk
		if end > len(data) {
			end = len(data)
		}
		payload := data[offset:end]
		last := end == len(data)

		var flags byte
		switch {
		case first && last:
			flags = flagFirst | flagLast
		case first:
			flags = flagFirst
		case last:
			flags = flagLast
		default:
			flags = 0
		}
		frame := encodeFrame(seq, flags, payload)

		l.mu.Lock()
		act := l.active
		actSize := act.size
		l.mu.Unlock()

		// 仅在记录首帧前考虑滚动；且当前分段确实放不下这一帧。
		willRoll := first && actSize > 0 && actSize+int64(len(frame)) > l.cfg.MaxSegmentBytes
		if err := l.ensureCapacity(len(frame), willRoll); err != nil {
			return 0, err
		}
		if willRoll {
			if err := l.roll(seq); err != nil {
				return 0, err
			}
			l.mu.Lock()
			act = l.active
			l.mu.Unlock()
			writeOff = 0
		} else {
			writeOff = actSize
		}

		// 持有目标分段写锁覆盖整条记录的全部帧：读取方读帧时持读锁，
		// 由此建立写入对已提交字节的 happens-before 关系（内存与文件
		// 内容都不会被半写帧观察到）。记录不跨分段，故锁只需取一次。
		if first {
			act.rwMu.Lock()
			defer act.rwMu.Unlock()
		}

		// 使用 WriteAt（pwrite）：带显式偏移；读取方经读锁只读取
		// 已提交区域，二者互不相交且由 rwMu 建立同步关系。
		n, werr := act.writeFile.WriteAt(frame, writeOff)
		l.mu.Lock()
		if l.active == act {
			l.active.size += int64(n)
		}
		l.totalBytes += int64(n)
		l.mu.Unlock()
		if werr != nil {
			return 0, werr
		}
		writeOff += int64(n)

		first = false
		offset = end
	}
	return seq, nil
}

// ensureCapacity 在写入前检查分段数/总字节上限，不满足则尝试回收；
// 回收后仍不满足返回 ErrStorageLimit（明确拒绝，而非阻塞或丢弃）。
// willRoll 表示本帧之前会新开一个分段（占用一个分段名额）。
func (l *Log) ensureCapacity(need int, willRoll bool) error {
	l.mu.Lock()
	segCount := len(l.segments)
	overSegments := willRoll && l.cfg.MaxSegments > 0 && segCount >= l.cfg.MaxSegments
	overBytes := l.cfg.MaxTotalBytes > 0 && l.totalBytes+int64(need) > l.cfg.MaxTotalBytes
	needReclaim := overSegments || overBytes
	l.mu.Unlock()
	if !needReclaim {
		return nil
	}
	// 尽力回收旧分段。
	if _, _, err := l.Reclaim(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	segCount = len(l.segments)
	overSegments = willRoll && l.cfg.MaxSegments > 0 && segCount >= l.cfg.MaxSegments
	overBytes = l.cfg.MaxTotalBytes > 0 && l.totalBytes+int64(need) > l.cfg.MaxTotalBytes
	if overSegments || overBytes {
		maxSeg := l.cfg.MaxSegments
		if maxSeg == 0 {
			maxSeg = -1
		}
		maxBytes := l.cfg.MaxTotalBytes
		if maxBytes == 0 {
			maxBytes = -1
		}
		return fmt.Errorf("%w: segments=%d/%d bytes=%d/%d",
			ErrStorageLimit, segCount, maxSeg, l.totalBytes, maxBytes)
	}
	return nil
}

// roll 关闭当前活动分段（fsync），创建以 seq 为 base 的新活动分段。
func (l *Log) roll(seq uint64) error {
	l.mu.Lock()
	old := l.active
	startOffset := old.startOffset + old.size
	l.mu.Unlock()

	if err := old.writeFile.Sync(); err != nil {
		return err
	}
	// 关闭旧活动分段的写句柄；读取方按需通过 fileCache 打开独立只读 fd。
	if err := old.closeWrite(); err != nil {
		return err
	}
	seg, err := createSegment(l.dir, seq, startOffset)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.segments = append(l.segments, seg)
	l.active = seg
	l.mu.Unlock()
	return nil
}

// broadcast 唤醒所有阻塞读取；关闭时用 closedNotify。
func (l *Log) broadcast(ch chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	close(ch)
	l.notify = make(chan struct{})
}

// waitCommit 阻塞直到 committedPosition > pos 或 ctx 结束或日志关闭。
func (l *Log) waitCommit(ctx context.Context, pos uint64) (uint64, error) {
	for {
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return 0, ErrClosed
		}
		if l.committedPosition > pos {
			cp := l.committedPosition
			l.mu.Unlock()
			return cp, nil
		}
		ch := l.notify
		l.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// CommittedPosition 返回已提交位置（下一条待写入序号）。
func (l *Log) CommittedPosition() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.committedPosition
}

// FirstPosition 返回当前仍可读取的最早位置。
func (l *Log) FirstPosition() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.firstPosition
}

// Sync 强制将活动分段缓冲刷入磁盘。
func (l *Log) Sync() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	act := l.active
	l.mu.Unlock()
	return act.writeFile.Sync()
}

// Close 刷盘并关闭全部分段句柄。之后任何操作返回 ErrClosed。
func (l *Log) Close() error {
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	l.closed = true
	act := l.active
	ch := l.notify
	readers := l.readers
	l.mu.Unlock()

	close(ch)
	var firstErr error
	if act != nil && act.writeFile != nil {
		if err := act.writeFile.Sync(); err != nil {
			firstErr = err
		}
	}
	l.cache.closeAll(l.segments)
	// 关闭活动分段写句柄。
	if act != nil {
		if err := act.closeWrite(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// 唤醒所有读取方使其退出。
	for r := range readers {
		r.markClosed()
	}
	return firstErr
}

var (
	_ io.Closer = (*Log)(nil)
	_ io.Closer = (*Reader)(nil)
)

var _ = os.O_RDWR

// NewReader 创建一个从指定位置开始读取的读取方。
// pos 为 0 且目录为空历史时从最早可读取位置开始；通常应传入
// FirstPosition() 或此前拿到的记录位置+1。
func (l *Log) NewReader(pos uint64) (*Reader, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrClosed
	}
	// pos 必须位于 [FirstPosition(), CommittedPosition()]；
	// 想从当前最早位置开始请传 FirstPosition()。
	if pos < l.firstPosition || pos > l.committedPosition {
		return nil, fmt.Errorf("%w: position %d outside [%d,%d]",
			ErrBadPosition, pos, l.firstPosition, l.committedPosition)
	}
	idx := l.segmentIndexLocked(pos)
	var off int64
	var err error
	if pos < l.committedPosition {
		off, err = l.frameOffsetLocked(idx, pos)
		if err != nil {
			return nil, err
		}
	} else {
		// 位于活动末端（tip）：没有历史帧偏移，指向当前活动分段尾部，
		// 之后写入的新记录会从这里读取。
		idx = len(l.segments) - 1
		off = l.segments[idx].size
	}
	r := &Reader{
		log:    l,
		segIdx: idx,
		segOff: off,
		wake:   make(chan struct{}, 1),
	}
	r.pos.Store(pos)
	l.readers[r] = struct{}{}
	return r, nil
}

// segmentIndexLocked 返回 basePosition <= pos 的最后一个分段下标。
func (l *Log) segmentIndexLocked(pos uint64) int {
	idx := 0
	for i, s := range l.segments {
		if s.basePosition <= pos {
			idx = i
		} else {
			break
		}
	}
	return idx
}

// minReaderPositionLocked 返回所有存活读取方中的最小待读位置；
// 没有读取方时返回 committedPosition（表示写入点之前皆可回收）。
func (l *Log) minReaderPositionLocked() uint64 {
	min := l.committedPosition
	for r := range l.readers {
		if rp := r.pos.Load(); rp < min {
			min = rp
		}
	}
	return min
}

// Reclaim 回收“被所有读取方越过”且“早于保留水位”的旧分段。
// 活动分段永不回收。返回被回收的分段数与释放字节数。
// 正在被读取（句柄被 pin）的分段本轮跳过，下轮再回收。
func (l *Log) Reclaim() (int, int64, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return 0, 0, ErrClosed
	}
	minPos := l.minReaderPositionLocked()
	watermark := int64(0)
	if l.cfg.RetainBytes > 0 {
		watermark = l.totalBytes - l.cfg.RetainBytes
	}

	type victim struct {
		idx int
		seg *segment
	}
	var victims []victim
	// 至少保留最后一个（活动）分段。
	for i := 0; i < len(l.segments)-1; i++ {
		s := l.segments[i]
		// 所有读取方的下一条待读记录都已越过本分段：
		// 即本分段最后一条记录序号 < minPos。由于下一分段 base 即
		// 本分段之后第一条记录序号，用 next.base <= minPos 判定最稳妥。
		nextBase := l.segments[i+1].basePosition
		if nextBase > minPos {
			continue
		}
		// 保留水位：分段起始偏移必须早于水位。
		if s.startOffset >= watermark && watermark > 0 {
			continue
		}
		victims = append(victims, victim{i, s})
	}
	l.mu.Unlock()

	var removed int
	var freed int64
	deleted := make(map[*segment]bool)
	for _, v := range victims {
		if err := l.cache.remove(v.seg); err != nil {
			// 仍被读取方占用：跳过，不报错。
			continue
		}
		if err := os.Remove(v.seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, freed, err
		}
		deleted[v.seg] = true
		removed++
		freed += v.seg.size
	}
	if removed == 0 {
		return 0, 0, nil
	}
	if err := syncDir(l.dir); err != nil {
		return removed, freed, err
	}

	// 更新内存视图：移除已删除分段并重排偏移。
	l.mu.Lock()
	kept := make([]*segment, 0, len(l.segments)-removed)
	for _, s := range l.segments {
		if !deleted[s] {
			kept = append(kept, s)
		}
	}
	l.segments = kept
	var off int64
	for _, s := range l.segments {
		s.startOffset = off
		off += s.size
	}
	l.totalBytes -= freed
	if len(l.segments) > 0 {
		l.firstPosition = l.segments[0].basePosition
	}
	l.mu.Unlock()
	return removed, freed, nil
}

// unregisterReader 从日志注销读取方；其位置不再阻止旧分段回收。
func (l *Log) unregisterReader(r *Reader) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	close(r.wake)
	r.mu.Unlock()

	l.mu.Lock()
	delete(l.readers, r)
	l.mu.Unlock()
}

// frameOffsetLocked 返回分段 idx 内记录序号 pos 的首帧字节偏移。
// 仅需顺序扫描该分段直到命中（新 Reader 创建时一次性开销）；
// 分段首帧必为首帧标志，序号单调递增。
func (l *Log) frameOffsetLocked(idx int, pos uint64) (int64, error) {
	if int(idx) >= len(l.segments) {
		return 0, ErrBadPosition
	}
	seg := l.segments[idx]
	if seg.basePosition == pos {
		return 0, nil
	}
	var off int64
	for off < seg.size {
		var hdr [frameHeaderSize]byte
		if seg.readFile == nil {
			// 只读分段可能已被换出，临时打开读取（不经 cache，避免锁重入）。
			f, err := os.OpenFile(seg.path, os.O_RDONLY, 0)
			if err != nil {
				return 0, err
			}
			n, err := f.ReadAt(hdr[:], off)
			_ = f.Close()
			if err != nil || n < frameHeaderSize {
				return 0, ErrIncompleteRecord
			}
		} else {
			n, err := seg.readFile.ReadAt(hdr[:], off)
			if err != nil || n < frameHeaderSize {
				return 0, ErrIncompleteRecord
			}
		}
		h, _ := parseFrameHeader(hdr[:])
		frameLen := frameHeaderSize + int64(h.dataLen)
		if h.flags&flagFirst != 0 {
			if h.seq == pos {
				return off, nil
			}
			if h.seq > pos {
				return 0, ErrBadPosition
			}
		}
		off += frameLen
	}
	return 0, ErrBadPosition
}
