package seglog

import (
	"errors"
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
	MaxTotalBytes    int64  // 数据目录总字节上限，超过且回收不出空间时返回 ErrNoCapacity
	MaxSegments      int    // 段数量上限，超过且回收不出空间时返回 ErrNoCapacity
	MaxOpenFiles     int    // 同时打开的段文件句柄上限（LRU 淘汰）
}

func (c *Config) withDefaults() {
	if c.MaxSegmentBytes <= 0 {
		c.MaxSegmentBytes = 64 << 20
	}
	if c.MaxSegmentBytes < 64 {
		c.MaxSegmentBytes = 64 // 至少容得下一个块头 + 1 字节负载
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
	if c.MaxTotalBytes <= 0 {
		c.MaxTotalBytes = 1 << 30
	}
	if c.MaxSegments <= 0 {
		c.MaxSegments = 1024
	}
	if c.MaxOpenFiles <= 0 {
		c.MaxOpenFiles = 64
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
	mu       sync.Mutex
	cfg      Config
	segs     []*segment         // 按 base 升序
	next     uint64             // 下一条记录的逻辑偏移
	inflight int64              // 自上次刷盘以来追加的字节数
	total    int64              // 全部段文件字节数之和
	pool     *filePool          // 有上限的段文件句柄池
	readers  map[*Reader]uint64 // 已注册读取方 -> 已消费位置（下一条待读偏移）
	slots    chan struct{}
	closed   bool
}

// Open 打开（必要时创建）目录中的日志，并执行崩溃恢复。
// 返回日志实例与恢复结论；正常关闭与异常终止对同一份数据结论一致。
func Open(cfg Config) (*Log, *RecoveryInfo, error) {
	cfg.withDefaults()
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, nil, err
	}
	ids, err := listSegmentIDs(cfg.Dir)
	if err != nil {
		return nil, nil, err
	}
	l := &Log{
		cfg:     cfg,
		pool:    newFilePool(cfg.MaxOpenFiles),
		slots:   make(chan struct{}, cfg.MaxReaders),
		readers: make(map[*Reader]uint64),
	}
	for _, id := range ids {
		seg, err := openSegment(l.pool, cfg.Dir, id.base, id.seq)
		if err != nil {
			return nil, nil, err
		}
		l.segs = append(l.segs, seg)
	}
	info, err := l.recover()
	if err != nil {
		l.Close()
		return nil, nil, err
	}
	// 崩溃可能留下刚滚动出来、尚未写入任何块的空尾段，其 base 已失效，删除之。
	for len(l.segs) > 1 && l.segs[len(l.segs)-1].size == 0 {
		if err := l.segs[len(l.segs)-1].remove(); err != nil {
			l.Close()
			return nil, nil, err
		}
		l.segs = l.segs[:len(l.segs)-1]
	}
	if len(l.segs) == 0 {
		seg, err := createSegment(l.pool, cfg.Dir, l.next, 0)
		if err != nil {
			l.Close()
			return nil, nil, err
		}
		l.segs = append(l.segs, seg)
	}
	l.recomputeTotalLocked()
	return l, info, nil
}

// segID 标识一个段文件：逻辑 base + 序号。
type segID struct {
	base uint64
	seq  int
}

func listSegmentIDs(dir string) ([]segID, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []segID
	for _, e := range ents {
		if base, seq, ok := parseSegmentFileName(e.Name()); ok {
			ids = append(ids, segID{base: base, seq: seq})
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].base != ids[j].base {
			return ids[i].base < ids[j].base
		}
		return ids[i].seq < ids[j].seq
	})
	return ids, nil
}

func (l *Log) recomputeTotalLocked() {
	var total int64
	for _, s := range l.segs {
		total += s.size
	}
	l.total = total
}

// recover 逐段扫描记录/块，从未完成的尾部推导确定的可用范围并截断残余。
// 跨段记录只写进一部分块时，整体回退到其首块处截断，归类为 CorruptPartialRecord。
func (l *Log) recover() (*RecoveryInfo, error) {
	info := &RecoveryInfo{}
	var next uint64
	if len(l.segs) > 0 {
		next = l.segs[0].base // 首段文件名携带已被回收的前缀长度
	}
	var pending bool      // 有未完结的跨段记录
	var pendSeg int       // 首块所在段下标
	var pendByte int64    // 首块在段内的字节偏移
	var pendStarts uint64 // 首块之前本段已有的记录起始数
	var truncated int64

	// truncateAt 截断 segs[segIdx] 到 byteOff，并删除其后所有段。
	truncateAt := func(segIdx int, byteOff int64, starts uint64) error {
		seg := l.segs[segIdx]
		truncated += seg.size - byteOff
		if err := seg.truncateTo(byteOff); err != nil {
			return err
		}
		if err := seg.sync(); err != nil {
			return err
		}
		seg.count = starts
		for j := len(l.segs) - 1; j > segIdx; j-- {
			truncated += l.segs[j].size
			if err := l.segs[j].remove(); err != nil {
				return err
			}
			l.segs = l.segs[:j]
		}
		return nil
	}

	for i := 0; i < len(l.segs); i++ {
		seg := l.segs[i]
		var data []byte
		if seg.size > 0 {
			d, err := seg.readAt(0, int(seg.size))
			if err != nil {
				return nil, err
			}
			data = d
		}
		var off int
		var starts uint64
		seenStart := false // 本段是否已遇到记录起始（区分首段孤儿续块）
		for off < len(data) {
			magic, _, consumed, derr := decodeRecord(data[off:])
			if derr != nil {
				ce := derr.(*CorruptError)
				ce.Segment = filepath.Base(seg.path)
				ce.Offset = next
				if pending {
					// 半截跨段记录之后跟着无法解码的残余：整体回退到首块。
					info.Corrupt = CorruptPartialRecord
					next-- // 撤销首块已分配的偏移
					if err := truncateAt(pendSeg, pendByte, pendStarts); err != nil {
						return nil, err
					}
				} else {
					info.Corrupt = ce.Kind
					if err := truncateAt(i, int64(off), starts); err != nil {
						return nil, err
					}
				}
				info.TruncatedByte = truncated
				l.next = next
				info.NextOffset = next
				info.ValidEnd = next
				return info, nil
			}
			switch magic {
			case recordMagic, magicChunkFirst:
				if pending {
					// 前一条跨段记录未完结就出现新起始：残余，回退到首块。
					info.Corrupt = CorruptPartialRecord
					next--
					if err := truncateAt(pendSeg, pendByte, pendStarts); err != nil {
						return nil, err
					}
					info.TruncatedByte = truncated
					l.next = next
					info.NextOffset = next
					info.ValidEnd = next
					return info, nil
				}
				if magic == magicChunkFirst {
					pending = true
					pendSeg, pendByte, pendStarts = i, int64(off), starts
				}
				next++
				starts++
				seenStart = true
			case magicChunkMiddle, magicChunkLast:
				if !pending {
					if i == 0 && !seenStart {
						// 首段开头的孤儿续块：首块在已被回收的前段中，跳过。
						off += consumed
						continue
					}
					// 数据流中间出现孤儿续块：残余。
					info.Corrupt = CorruptPartialRecord
					if err := truncateAt(i, int64(off), starts); err != nil {
						return nil, err
					}
					info.TruncatedByte = truncated
					l.next = next
					info.NextOffset = next
					info.ValidEnd = next
					return info, nil
				}
				if magic == magicChunkLast {
					pending = false
				}
			}
			off += consumed
		}
		seg.count = starts
	}
	if pending {
		// 数据结束但跨段记录未完结：写到一半的残余，回退到首块。
		info.Corrupt = CorruptPartialRecord
		next--
		if err := truncateAt(pendSeg, pendByte, pendStarts); err != nil {
			return nil, err
		}
		info.TruncatedByte = truncated
	}
	l.next = next
	info.NextOffset = next
	info.ValidEnd = next
	return info, nil
}

// planChunks 预演追加 payloadLen 字节负载所需写入的总字节数与需新建的段数。
// 必须与 Append 的写循环保持一致。
func (l *Log) planChunks(payloadLen int) (needBytes int64, needSegs int) {
	cur := l.segs[len(l.segs)-1].size
	rem := payloadLen
	first := true
	for first || rem > 0 {
		space := l.cfg.MaxSegmentBytes - cur
		if space < recordHdrSize+1 && cur > 0 {
			needSegs++
			cur = 0
			continue
		}
		n := int(space) - recordHdrSize
		if n > rem {
			n = rem
		}
		if n < 0 {
			n = 0
		}
		needBytes += int64(recordHdrSize + n)
		cur += int64(recordHdrSize + n)
		rem -= n
		first = false
	}
	return needBytes, needSegs
}

// ensureCapacityLocked 校验容量上限；先按消费进度回收旧段，仍不够则返回 ErrNoCapacity。
func (l *Log) ensureCapacityLocked(needBytes int64, needSegs int) error {
	fits := func() bool {
		return l.total+needBytes <= l.cfg.MaxTotalBytes &&
			len(l.segs)+needSegs <= l.cfg.MaxSegments
	}
	if fits() {
		return nil
	}
	l.autoReclaimLocked()
	if !fits() {
		return ErrNoCapacity
	}
	return nil
}

// consumerHorizonLocked 返回所有已注册读取方的最小消费进度；无读取方时 ok=false。
func (l *Log) consumerHorizonLocked() (horizon uint64, ok bool) {
	if len(l.readers) == 0 {
		return 0, false
	}
	h := ^uint64(0)
	for _, c := range l.readers {
		if c < h {
			h = c
		}
	}
	return h, true
}

// autoReclaimLocked 按消费进度回收确定无人再要的旧段；无注册读取方时不回收。
func (l *Log) autoReclaimLocked() {
	h, ok := l.consumerHorizonLocked()
	if !ok {
		return
	}
	l.reclaimLocked(h)
}

// reclaimLocked 整体删除完全落在 before 之前的非活跃段。
func (l *Log) reclaimLocked(before uint64) error {
	for len(l.segs) > 1 {
		s := l.segs[0]
		if s.base+s.count > before {
			break
		}
		l.total -= s.size
		if err := s.remove(); err != nil {
			return err
		}
		l.segs = l.segs[1:]
	}
	return nil
}

// Append 追加一条记录，返回其逻辑偏移。
// 记录放不进当前段剩余空间时拆成连续多块跨段写入；容量不足返回 ErrNoCapacity。
func (l *Log) Append(payload []byte) (uint64, error) {
	if len(payload) > l.cfg.MaxRecordBytes {
		return 0, ErrRecordTooLarge
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	needBytes, needSegs := l.planChunks(len(payload))
	if l.inflight+needBytes > l.cfg.MaxInflightBytes {
		return 0, ErrBackpressure
	}
	if err := l.ensureCapacityLocked(needBytes, needSegs); err != nil {
		return 0, err
	}
	off := l.next
	rem := payload
	first := true
	for first || len(rem) > 0 {
		active := l.segs[len(l.segs)-1]
		space := l.cfg.MaxSegmentBytes - active.size
		if space < recordHdrSize+1 && active.size > 0 {
			// 新段逻辑 base = 之前各段已开始的记录数；若当前记录首块已落旧段，要算上它。
			// 一条记录跨 3 段以上时会出现连续的纯续块段，它们 base 相同，用 seq 区分。
			base := l.next
			if !first {
				base++
			}
			seq := 0
			if last := l.segs[len(l.segs)-1]; last.base == base {
				seq = last.seq + 1
			}
			seg, err := createSegment(l.pool, l.cfg.Dir, base, seq)
			if err != nil {
				return 0, err
			}
			l.segs = append(l.segs, seg)
			continue
		}
		n := int(space) - recordHdrSize
		if n > len(rem) {
			n = len(rem)
		}
		if n < 0 {
			n = 0
		}
		var magic uint32
		switch {
		case first && n == len(rem):
			magic = recordMagic
		case first:
			magic = magicChunkFirst
		case n == len(rem):
			magic = magicChunkLast
		default:
			magic = magicChunkMiddle
		}
		chunk := encodeChunk(magic, rem[:n])
		if _, err := active.append(chunk); err != nil {
			return 0, err
		}
		l.total += int64(len(chunk))
		l.inflight += int64(len(chunk))
		if first {
			active.count++
		}
		rem = rem[n:]
		first = false
	}
	l.next++
	if l.cfg.SyncOnAppend {
		if err := l.segs[len(l.segs)-1].sync(); err != nil {
			return 0, err
		}
		l.inflight = 0
	}
	return off, nil
}

// Read 读取逻辑偏移 off 处的完整记录；跨段记录在此拼装还原。
// 残余/损坏数据不会作为完整记录返回，错误原因可区分。
func (l *Log) Read(off uint64) ([]byte, error) {
	select {
	case l.slots <- struct{}{}:
		defer func() { <-l.slots }()
	default:
		return nil, ErrTooManyReaders
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, ErrClosed
	}
	if len(l.segs) > 0 && off < l.segs[0].base {
		l.mu.Unlock()
		return nil, ErrOffsetReclaimed
	}
	if off >= l.next {
		l.mu.Unlock()
		return nil, ErrOffsetOutOfRange
	}
	// 定位包含 off 记录首块的段：base <= off < base+count。
	idx := sort.Search(len(l.segs), func(i int) bool {
		s := l.segs[i]
		return off < s.base+s.count
	})
	if idx >= len(l.segs) || off < l.segs[idx].base {
		l.mu.Unlock()
		return nil, ErrOffsetOutOfRange
	}
	// 快照段列表与各段大小：off < next 保证记录已完整写入，快照覆盖其全部块。
	segs := append([]*segment(nil), l.segs[idx:]...)
	sizes := make([]int64, len(segs))
	for i, s := range segs {
		sizes[i] = s.size
	}
	l.mu.Unlock()

	logical := segs[0].base
	var assembled []byte
	inRecord := false // 已读到目标记录首块，正在拼续块
	for si, seg := range segs {
		if si > 0 && !inRecord {
			break // 只有拼装中的跨段记录才会进入后续段
		}
		var pos int64
		for pos < sizes[si] {
			hdr, err := seg.readAt(pos, recordHdrSize)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					// 快照之后该段被回收删除。
					return nil, ErrOffsetReclaimed
				}
				return nil, &CorruptError{Kind: CorruptTornTail, Segment: filepath.Base(seg.path), Offset: logical, Detail: "short header read"}
			}
			length := int64(hdr[4]) | int64(hdr[5])<<8 | int64(hdr[6])<<16 | int64(hdr[7])<<24
			total := int64(recordHdrSize) + length
			buf := make([]byte, total)
			copy(buf, hdr)
			rest, err := seg.readAt(pos+recordHdrSize, int(total)-recordHdrSize)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil, ErrOffsetReclaimed
				}
				return nil, &CorruptError{Kind: CorruptTornTail, Segment: filepath.Base(seg.path), Offset: logical, Detail: "short payload read"}
			}
			copy(buf[recordHdrSize:], rest)
			magic, payload, _, derr := decodeRecord(buf)
			if derr != nil {
				ce := derr.(*CorruptError)
				ce.Segment = filepath.Base(seg.path)
				ce.Offset = logical
				return nil, ce
			}
			if inRecord {
				if !isChunkContinuation(magic) {
					return nil, &CorruptError{Kind: CorruptPartialRecord, Segment: filepath.Base(seg.path), Offset: logical, Detail: "chunked record interrupted by new record"}
				}
				assembled = append(assembled, payload...)
				if magic == magicChunkLast {
					return assembled, nil
				}
			} else {
				switch magic {
				case recordMagic:
					if logical == off {
						out := make([]byte, len(payload))
						copy(out, payload)
						return out, nil
					}
					logical++
				case magicChunkFirst:
					if logical == off {
						assembled = append(assembled, payload...)
						inRecord = true
					}
					logical++
				default:
					// 段首孤儿续块（首块在已回收段中），属于 offset < base 的记录，跳过。
				}
			}
			pos += total
		}
	}
	return nil, &CorruptError{Kind: CorruptPartialRecord, Segment: filepath.Base(segs[len(segs)-1].path), Offset: off, Detail: "record chunks truncated"}
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
	return l.segs[0].base
}

// Reclaim 回收逻辑偏移 before 之前的旧段。
// 若存在已注册读取方，实际回收界被钳制到所有读取方的最小消费进度：
// 任何读取方还没越过的数据都不会被回收。
func (l *Log) Reclaim(before uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if h, ok := l.consumerHorizonLocked(); ok && h < before {
		before = h
	}
	return l.reclaimLocked(before)
}

// Sync 将当前活跃段刷盘，并清零在途字节计数。
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if len(l.segs) == 0 {
		return nil
	}
	if err := l.segs[len(l.segs)-1].sync(); err != nil {
		return err
	}
	l.inflight = 0
	return nil
}

// Close 关闭日志，释放全部文件句柄。
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	l.pool.closeAll()
	return nil
}
