package seglog

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Config 日志配置项。
type Config struct {
	Dir              string // 数据目录
	MaxSegmentBytes  int64  // 单段最大字节数，超过则滚动新段
	MaxRecordBytes   int    // 单条记录最大字节数
	MaxInflightBytes int64  // 在途未刷盘字节上限，超过返回 ErrBackpressure
	MaxReaders       int    // 并发读槽位上限，超过返回 ErrTooManyReaders
	SyncOnAppend     bool   // 每次追加后是否立即刷盘
	MaxTotalBytes    int64  // 数据目录总字节上限，0 表示不限；超限先按消费进度回收，仍不够返回 ErrNoCapacity
	MaxSegments      int    // 段文件数量上限（含续段），0 表示不限；超限处理同上
	MaxOpenFiles     int    // 同时打开的底层文件句柄上限，0 表示不限
}

func (c *Config) withDefaults() {
	if c.MaxSegmentBytes <= 0 {
		c.MaxSegmentBytes = 64 << 20
	}
	if c.MaxRecordBytes <= 0 {
		c.MaxRecordBytes = 4 << 20
	}
	if c.MaxInflightBytes <= 0 {
		c.MaxInflightBytes = 16 << 20
	}
	if c.MaxReaders <= 0 {
		c.MaxReaders = 128
	}
}

// RecoveryInfo 描述一次恢复得出的结论。
type RecoveryInfo struct {
	NextOffset    uint64      // 恢复后可写入的下一个逻辑偏移
	ValidEnd      uint64      // 恢复后可见数据的逻辑终点（不含）
	TruncatedByte int64       // 从尾部截掉的残余字节数
	Corrupt       CorruptKind // 截断原因（无损坏为 CorruptNone）
}

// Log 分段追加日志。
type Log struct {
	mu             sync.Mutex
	cfg            Config
	segs           []*segment // 按 (base, part) 升序
	next           uint64     // 下一条记录的逻辑偏移
	inflight       int64      // 自上次刷盘以来追加的字节数
	total          int64      // 全部段文件的字节总数
	readers        chan struct{}
	handles        handleCache
	consumers      map[uint64]uint64 // 消费者 ID -> 已消费到的逻辑偏移（不含）
	nextConsumerID uint64
	closed         bool
}

// Open 打开（必要时创建）目录中的日志，并执行崩溃恢复。
// 返回日志实例与恢复结论；正常关闭与异常终止对同一份数据结论一致。
func Open(cfg Config) (*Log, *RecoveryInfo, error) {
	cfg.withDefaults()
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, nil, err
	}
	segIDs, err := listSegmentBases(cfg.Dir)
	if err != nil {
		return nil, nil, err
	}
	l := &Log{
		cfg:       cfg,
		readers:   make(chan struct{}, cfg.MaxReaders),
		handles:   handleCache{cap: cfg.MaxOpenFiles},
		consumers: make(map[uint64]uint64),
	}
	for _, id := range segIDs {
		seg, err := openSegment(cfg.Dir, id.base, id.part)
		if err != nil {
			return nil, nil, err
		}
		l.segs = append(l.segs, seg)
		l.total += seg.size
	}
	info, err := l.recover()
	if err != nil {
		l.Close()
		return nil, nil, err
	}
	if len(l.segs) == 0 {
		seg, err := createSegment(cfg.Dir, 0, 0)
		if err != nil {
			return nil, nil, err
		}
		l.handles.adopt(seg)
		l.segs = append(l.segs, seg)
	}
	return l, info, nil
}

type segmentID struct {
	base uint64
	part uint16
}

func listSegmentBases(dir string) ([]segmentID, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []segmentID
	for _, e := range ents {
		if base, part, ok := parseSegmentFileName(e.Name()); ok {
			ids = append(ids, segmentID{base: base, part: part})
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].base != ids[j].base {
			return ids[i].base < ids[j].base
		}
		return ids[i].part < ids[j].part
	})
	return ids, nil
}

// recover 逐段扫描记录与分片，从未完成的尾部推导确定的可用范围并截断残余。
// 跨段记录只有首分片到最后分片完整连续才被计数；写到一半的跨段记录
// 归类为 CorruptPartialRecord，从其首分片处整体截断（可能跨回之前的段）。
func (l *Log) recover() (*RecoveryInfo, error) {
	info := &RecoveryInfo{}
	var (
		reassembling bool  // 正在重组一条跨段记录
		firstSeg     int   // 首分片所在段索引
		firstPos     int64 // 首分片在段内的字节位置
	)
	// truncate 把 segIdx 段截到 pos 字节，并删除其后所有段；返回截掉的字节数。
	truncate := func(segIdx int, pos int64) (int64, error) {
		var removed int64
		ts := l.segs[segIdx]
		removed += ts.size - pos
		if _, err := l.handles.acquire(ts); err != nil {
			return 0, err
		}
		if err := ts.truncateTo(pos); err != nil {
			l.handles.release(ts)
			return 0, err
		}
		if err := ts.sync(); err != nil {
			l.handles.release(ts)
			return 0, err
		}
		l.handles.release(ts)
		for j := len(l.segs) - 1; j > segIdx; j-- {
			removed += l.segs[j].size
			l.handles.evict(l.segs[j])
			if err := l.segs[j].remove(); err != nil {
				return 0, err
			}
			l.segs = l.segs[:j]
		}
		l.total -= removed
		return removed, nil
	}

	var corrupt *CorruptError
	truncSeg, truncPos := 0, int64(0)
	for i := 0; i < len(l.segs) && corrupt == nil; i++ {
		seg := l.segs[i]
		if _, err := l.handles.acquire(seg); err != nil {
			return nil, err
		}
		data, err := io.ReadAll(io.NewSectionReader(seg.file, 0, seg.size))
		l.handles.release(seg)
		if err != nil {
			return nil, err
		}
		var off int
		for off < len(data) {
			if reassembling {
				// 跨段记录写了一半，后续必须是它的后续分片。
				flags, _, consumed, derr := decodeFragment(data[off:])
				if derr != nil || flags&fragFirst != 0 {
					detail := "record split across segments left incomplete"
					if derr != nil {
						detail += ": " + derr.(*CorruptError).Detail
					}
					corrupt = &CorruptError{Kind: CorruptPartialRecord, Segment: filepath.Base(l.segs[firstSeg].path), Offset: l.segs[firstSeg].base, Detail: detail}
					truncSeg, truncPos = firstSeg, firstPos
					break
				}
				if flags&fragLast != 0 {
					reassembling = false
					l.segs[firstSeg].count++ // 记录完成，计到首分片所在段
				}
				off += consumed
				continue
			}
			if len(data)-off >= 4 && binary.LittleEndian.Uint32(data[off:off+4]) == fragMagic {
				flags, _, consumed, derr := decodeFragment(data[off:])
				if derr != nil {
					ce := derr.(*CorruptError)
					corrupt = &CorruptError{Kind: ce.Kind, Segment: filepath.Base(seg.path), Offset: seg.base + seg.count, Detail: ce.Detail}
					truncSeg, truncPos = i, int64(off)
					break
				}
				if flags&fragFirst == 0 {
					if !seg.continued {
						// 普通段里出现没有首分片的后续分片：残余。
						corrupt = &CorruptError{Kind: CorruptPartialRecord, Segment: filepath.Base(seg.path), Offset: seg.base + seg.count, Detail: "continuation fragment without start"}
						truncSeg, truncPos = i, int64(off)
						break
					}
					// 续段开头：首分片已被回收的记录残余，跳过。
					off += consumed
					continue
				}
				if flags&fragLast != 0 {
					seg.count++ // 单片记录（容忍）
				} else {
					reassembling = true
					firstSeg, firstPos = i, int64(off)
				}
				off += consumed
				continue
			}
			_, consumed, derr := decodeRecord(data[off:])
			if derr != nil {
				ce := derr.(*CorruptError)
				corrupt = &CorruptError{Kind: ce.Kind, Segment: filepath.Base(seg.path), Offset: seg.base + seg.count, Detail: ce.Detail}
				truncSeg, truncPos = i, int64(off)
				break
			}
			seg.count++
			off += consumed
		}
	}
	if corrupt == nil && reassembling {
		// 数据结束但跨段记录没写完：写到一半的残余。
		corrupt = &CorruptError{Kind: CorruptPartialRecord, Segment: filepath.Base(l.segs[firstSeg].path), Offset: l.segs[firstSeg].base, Detail: "record split across segments left incomplete at end of log"}
		truncSeg, truncPos = firstSeg, firstPos
	}
	if corrupt != nil {
		removed, err := truncate(truncSeg, truncPos)
		if err != nil {
			return nil, err
		}
		info.TruncatedByte = removed
		info.Corrupt = corrupt.Kind
	}
	if len(l.segs) > 0 {
		l.next = l.segs[len(l.segs)-1].end()
	}
	info.NextOffset = l.next
	info.ValidEnd = l.next
	return info, nil
}

// Append 追加一条记录，返回其逻辑偏移。
// 记录编码后超过单段上限时，拆成连续分片写入后续分段，读侧仍视为一条完整记录。
func (l *Log) Append(payload []byte) (uint64, error) {
	if len(payload) > l.cfg.MaxRecordBytes {
		return 0, ErrRecordTooLarge
	}
	rec := encodeRecord(payload)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	if int64(len(rec)) > l.cfg.MaxSegmentBytes {
		return l.appendSplitLocked(payload)
	}
	if l.inflight+int64(len(rec)) > l.cfg.MaxInflightBytes {
		return 0, ErrBackpressure
	}
	active := l.segs[len(l.segs)-1]
	newSegs := 0
	if active.size > 0 && active.size+int64(len(rec)) > l.cfg.MaxSegmentBytes {
		newSegs = 1
	}
	if err := l.ensureCapacityLocked(int64(len(rec)), newSegs); err != nil {
		return 0, err
	}
	if newSegs == 1 {
		seg, err := l.rollSegmentLocked(l.next, 0)
		if err != nil {
			return 0, err
		}
		active = seg
	}
	if ok, err := l.handles.acquire(active); err != nil {
		return 0, err
	} else if !ok {
		return 0, ErrNoCapacity // 句柄上限内拿不到可写句柄
	}
	if _, err := active.append(rec); err != nil {
		l.handles.release(active)
		return 0, err
	}
	off := l.next
	l.next++
	l.inflight += int64(len(rec))
	l.total += int64(len(rec))
	if l.cfg.SyncOnAppend {
		if err := active.sync(); err != nil {
			l.handles.release(active)
			return 0, err
		}
		l.inflight = 0
	}
	l.handles.release(active)
	return off, nil
}

// appendSplitLocked 把一条放不进单段的记录拆成连续分片写入后续分段。
// 首分片段名与普通段相同（part=0），续段文件名带分片序号；
// 只有全部分片写完，记录才计入首分片段的 count 并提交逻辑偏移。
func (l *Log) appendSplitLocked(payload []byte) (uint64, error) {
	chunkCap := int(l.cfg.MaxSegmentBytes) - fragHdrSize
	if chunkCap <= 0 {
		return 0, ErrRecordTooLarge // 段连一个分片都放不下
	}
	nFrag := (len(payload) + chunkCap - 1) / chunkCap
	need := int64(len(payload)) + int64(nFrag)*fragHdrSize
	if l.inflight+need > l.cfg.MaxInflightBytes {
		return 0, ErrBackpressure
	}
	active := l.segs[len(l.segs)-1]
	newSegs := nFrag
	if active.size == 0 {
		newSegs-- // 活跃段为空，首分片直接复用它
	}
	if err := l.ensureCapacityLocked(need, newSegs); err != nil {
		return 0, err
	}
	off := l.next
	var first *segment
	var touched []*segment
	pos := 0
	for i := 0; i < nFrag; i++ {
		end := pos + chunkCap
		if end > len(payload) {
			end = len(payload)
		}
		flags := uint8(0)
		if i == 0 {
			flags |= fragFirst
		}
		if i == nFrag-1 {
			flags |= fragLast
		}
		frag := encodeFragment(flags, payload[pos:end])
		pos = end
		var seg *segment
		if i == 0 && active.size == 0 {
			seg = active // 活跃段为空，直接写首分片
		} else {
			var err error
			seg, err = l.rollSegmentLocked(off, uint16(i))
			if err != nil {
				return 0, err
			}
		}
		if i == 0 {
			first = seg
		}
		if ok, err := l.handles.acquire(seg); err != nil {
			return 0, err
		} else if !ok {
			return 0, ErrNoCapacity
		}
		if _, err := seg.appendRaw(frag); err != nil {
			l.handles.release(seg)
			return 0, err
		}
		l.inflight += int64(len(frag))
		l.total += int64(len(frag))
		touched = append(touched, seg)
	}
	first.count++ // 记录完成，计到首分片所在段
	l.next++
	if l.cfg.SyncOnAppend {
		for _, seg := range touched {
			if err := seg.sync(); err != nil {
				l.handles.release(seg)
				return 0, err
			}
			l.handles.release(seg)
		}
		l.inflight = 0
	} else {
		for _, seg := range touched {
			l.handles.release(seg)
		}
	}
	return off, nil
}

// rollSegmentLocked 新建一个段并追加到段序列尾部。
// 句柄上限内登记不下时清理新段并返回 ErrNoCapacity。
func (l *Log) rollSegmentLocked(base uint64, part uint16) (*segment, error) {
	seg, err := createSegment(l.cfg.Dir, base, part)
	if err != nil {
		return nil, err
	}
	if !l.handles.adopt(seg) {
		seg.remove()
		return nil, ErrNoCapacity
	}
	l.segs = append(l.segs, seg)
	return seg, nil
}

// ensureCapacityLocked 检查容量上限；超限先按消费进度回收，仍不够返回 ErrNoCapacity。
func (l *Log) ensureCapacityLocked(need int64, newSegs int) error {
	if l.overCapacityLocked(need, newSegs) {
		if err := l.reclaimLocked(l.horizonLocked()); err != nil {
			return err
		}
		if l.overCapacityLocked(need, newSegs) {
			return ErrNoCapacity
		}
	}
	return nil
}

func (l *Log) overCapacityLocked(need int64, newSegs int) bool {
	if l.cfg.MaxTotalBytes > 0 && l.total+need > l.cfg.MaxTotalBytes {
		return true
	}
	if l.cfg.MaxSegments > 0 && len(l.segs)+newSegs > l.cfg.MaxSegments {
		return true
	}
	return false
}

// horizonLocked 返回按消费进度可回收到的逻辑偏移（不含）：
// 所有注册消费者已消费位置的最小值；没有注册消费者时视为全部可消费进度已到最新。
func (l *Log) horizonLocked() uint64 {
	h := l.next
	for _, committed := range l.consumers {
		if committed < h {
			h = committed
		}
	}
	return h
}

// Read 读取逻辑偏移 off 处的完整记录。
// 跨段记录在读取时重组为一条完整记录返回；只写了一半的跨段记录
// 永远不会作为完整记录返回（恢复时已截断）。残余/损坏数据的错误原因可区分。
func (l *Log) Read(off uint64) ([]byte, error) {
	select {
	case l.readers <- struct{}{}:
		defer func() { <-l.readers }()
	default:
		return nil, ErrTooManyReaders
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, ErrClosed
	}
	if len(l.segs) > 0 {
		first := l.segs[0].base
		if l.segs[0].continued {
			first++
		}
		if off < first {
			l.mu.Unlock()
			return nil, ErrOffsetReclaimed
		}
	}
	if off >= l.next {
		l.mu.Unlock()
		return nil, ErrOffsetOutOfRange
	}
	// 定位包含 off 起始位置的段：base <= off < end()。
	idx := sort.Search(len(l.segs), func(i int) bool { return off < l.segs[i].end() })
	if idx >= len(l.segs) || off < l.segs[idx].base {
		l.mu.Unlock()
		return nil, ErrOffsetOutOfRange
	}
	// 快照段序列与各段尺寸（跨段记录要跟随到后续段）。
	segs := make([]*segment, len(l.segs)-idx)
	sizes := make([]int64, len(l.segs)-idx)
	for i := idx; i < len(l.segs); i++ {
		segs[i-idx] = l.segs[i]
		sizes[i-idx] = l.segs[i].size
	}
	l.mu.Unlock()

	logical := segs[0].base
	var assembled []byte
	assembling := false
	for si, seg := range segs {
		ok, err := l.handles.acquire(seg)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, ErrOffsetReclaimed // 扫描期间被回收
			}
			return nil, err
		}
		if !ok {
			return nil, ErrTooManyReaders
		}
		data, err := io.ReadAll(io.NewSectionReader(seg.file, 0, sizes[si]))
		l.handles.release(seg)
		if err != nil {
			return nil, err
		}
		var pos int
		for pos < len(data) {
			if assembling {
				// 跨段记录的后续分片。
				flags, chunk, consumed, derr := decodeFragment(data[pos:])
				if derr != nil {
					return nil, withSeg(derr, seg, logical)
				}
				if flags&fragFirst != 0 {
					return nil, &CorruptError{Kind: CorruptPartialRecord, Segment: filepath.Base(seg.path), Offset: logical, Detail: "new record started before split record completed"}
				}
				assembled = append(assembled, chunk...)
				pos += consumed
				if flags&fragLast != 0 {
					if logical == off {
						return assembled, nil
					}
					assembled = assembled[:0]
					assembling = false
					logical++
				}
				continue
			}
			if len(data)-pos >= 4 && binary.LittleEndian.Uint32(data[pos:pos+4]) == fragMagic {
				flags, chunk, consumed, derr := decodeFragment(data[pos:])
				if derr != nil {
					return nil, withSeg(derr, seg, logical)
				}
				pos += consumed
				switch {
				case flags&fragFirst != 0 && flags&fragLast != 0:
					if logical == off {
						out := make([]byte, len(chunk))
						copy(out, chunk)
						return out, nil
					}
					logical++
				case flags&fragFirst != 0:
					assembled = append(assembled[:0], chunk...)
					assembling = true
				default:
					// 续段开头：属于在之前的段里起始的记录，跳过。
					if !seg.continued {
						return nil, &CorruptError{Kind: CorruptPartialRecord, Segment: filepath.Base(seg.path), Offset: logical, Detail: "continuation fragment without start"}
					}
					if flags&fragLast != 0 {
						logical++
					}
				}
				continue
			}
			payload, consumed, derr := decodeRecord(data[pos:])
			if derr != nil {
				return nil, withSeg(derr, seg, logical)
			}
			if logical == off {
				out := make([]byte, len(payload))
				copy(out, payload)
				return out, nil
			}
			logical++
			pos += consumed
		}
	}
	// 已提交记录的分片在写入时连续落盘，走到这里说明数据不完整。
	return nil, &CorruptError{Kind: CorruptPartialRecord, Segment: filepath.Base(segs[len(segs)-1].path), Offset: logical, Detail: "split record incomplete"}
}

func withSeg(err error, seg *segment, off uint64) error {
	ce := err.(*CorruptError)
	ce.Segment = filepath.Base(seg.path)
	ce.Offset = off
	return ce
}

// NextOffset 返回下一条将被写入的逻辑偏移。
func (l *Log) NextOffset() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next
}

// FirstOffset 返回当前可见的最小逻辑偏移（回收后可能 > 0）。
func (l *Log) FirstOffset() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segs) == 0 {
		return 0
	}
	f := l.segs[0].base
	if l.segs[0].continued {
		f++ // 首段是续段：被续写记录的首分片已回收
	}
	return f
}

// RegisterConsumer 注册一个消费者，at 为其已消费到的逻辑偏移（不含）。
// 返回的消费者句柄用于 Commit / UnregisterConsumer。
func (l *Log) RegisterConsumer(at uint64) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	if at > l.next {
		at = l.next
	}
	l.nextConsumerID++
	l.consumers[l.nextConsumerID] = at
	return l.nextConsumerID, nil
}

// Commit 上报消费者已消费到的逻辑偏移（不含）。进度只前进不回退。
// 回收只会清掉所有注册消费者都已越过的数据。
func (l *Log) Commit(id, off uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	cur, ok := l.consumers[id]
	if !ok {
		return ErrUnknownConsumer
	}
	if off > l.next {
		off = l.next
	}
	if off > cur {
		l.consumers[id] = off
	}
	return nil
}

// UnregisterConsumer 注销消费者，其进度不再阻碍回收。
func (l *Log) UnregisterConsumer(id uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if _, ok := l.consumers[id]; !ok {
		return ErrUnknownConsumer
	}
	delete(l.consumers, id)
	return nil
}

// ReclaimableOffset 返回当前按消费进度可回收到的逻辑偏移（不含）。
func (l *Log) ReclaimableOffset() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.horizonLocked()
}

// Reclaim 回收逻辑偏移 before 之前的旧段。
// 实际回收范围不超过所有注册消费者都已越过的位置（消费进度地平线）；
// 只整体删除完全落在该范围之前的段，仍被读取位置引用的数据不会被回收。
func (l *Log) Reclaim(before uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if h := l.horizonLocked(); before > h {
		before = h
	}
	return l.reclaimLocked(before)
}

// reclaimLocked 删除完全落在 before 之前的非活跃段；正被读取钉住的段跳过。
func (l *Log) reclaimLocked(before uint64) error {
	// 保留活跃段，避免破坏正在写入的位置。
	for len(l.segs) > 1 {
		s := l.segs[0]
		if s.end() > before {
			break
		}
		if l.handles.pinned(s) {
			break // 正被读取，留待下次回收
		}
		l.handles.evict(s)
		l.total -= s.size
		if err := s.remove(); err != nil {
			return err
		}
		l.segs = l.segs[1:]
	}
	return nil
}

// Sync 将所有段的未刷盘数据刷盘，并清零在途字节计数。
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	for _, s := range l.segs {
		ok, err := l.handles.acquire(s)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNoCapacity // 句柄上限内拿不到句柄，不假装已刷盘
		}
		if err := s.sync(); err != nil {
			l.handles.release(s)
			return err
		}
		l.handles.release(s)
	}
	l.inflight = 0
	return nil
}

// Close 关闭日志，释放文件句柄。
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	for _, s := range l.segs {
		l.handles.evict(s)
	}
	return nil
}
