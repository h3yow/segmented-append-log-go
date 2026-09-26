package seglog

import (
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
	mu       sync.Mutex
	cfg      Config
	segs     []*segment // 按 base 升序
	next     uint64     // 下一条记录的逻辑偏移
	inflight int64      // 自上次刷盘以来追加的字节数
	readers  chan struct{}
	closed   bool
}

// Open 打开（必要时创建）目录中的日志，并执行崩溃恢复。
// 返回日志实例与恢复结论；正常关闭与异常终止对同一份数据结论一致。
func Open(cfg Config) (*Log, *RecoveryInfo, error) {
	cfg.withDefaults()
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, nil, err
	}
	bases, err := listSegmentBases(cfg.Dir)
	if err != nil {
		return nil, nil, err
	}
	l := &Log{cfg: cfg, readers: make(chan struct{}, cfg.MaxReaders)}
	for _, base := range bases {
		seg, err := openSegment(cfg.Dir, base)
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
	if len(l.segs) == 0 {
		seg, err := createSegment(cfg.Dir, 0)
		if err != nil {
			return nil, nil, err
		}
		l.segs = append(l.segs, seg)
	}
	return l, info, nil
}

func listSegmentBases(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []uint64
	for _, e := range ents {
		if base, ok := parseSegmentFileName(e.Name()); ok {
			bases = append(bases, base)
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

// recover 逐段扫描记录，从未完成的尾部推导确定的可用范围并截断残余。
func (l *Log) recover() (*RecoveryInfo, error) {
	info := &RecoveryInfo{}
	for i := 0; i < len(l.segs); i++ {
		seg := l.segs[i]
		data, err := io.ReadAll(io.NewSectionReader(seg.file, 0, seg.size))
		if err != nil {
			return nil, err
		}
		var off int
		var count uint64
		var corrupt *CorruptError
		for off < len(data) {
			_, consumed, derr := decodeRecord(data[off:])
			if derr != nil {
				corrupt = &CorruptError{}
				*corrupt = *(derr.(*CorruptError))
				corrupt.Segment = filepath.Base(seg.path)
				corrupt.Offset = seg.base + count
				break
			}
			off += consumed
			count++
		}
		if corrupt != nil {
			// 截断本段尾部残余，并删除其后所有段，保证结论确定。
			info.TruncatedByte = seg.size - int64(off)
			info.Corrupt = corrupt.Kind
			if err := seg.truncateTo(int64(off)); err != nil {
				return nil, err
			}
			if err := seg.sync(); err != nil {
				return nil, err
			}
			for j := len(l.segs) - 1; j > i; j-- {
				if err := l.segs[j].remove(); err != nil {
					return nil, err
				}
				l.segs = l.segs[:j]
			}
			seg.count = count
			l.next = seg.base + count
			info.NextOffset = l.next
			info.ValidEnd = l.next
			return info, nil
		}
		seg.count = count
	}
	if len(l.segs) > 0 {
		last := l.segs[len(l.segs)-1]
		l.next = last.base + last.count
	}
	info.NextOffset = l.next
	info.ValidEnd = l.next
	return info, nil
}

// Append 追加一条记录，返回其逻辑偏移。
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
	if l.inflight+int64(len(rec)) > l.cfg.MaxInflightBytes {
		return 0, ErrBackpressure
	}
	active := l.segs[len(l.segs)-1]
	if active.size > 0 && active.size+int64(len(rec)) > l.cfg.MaxSegmentBytes {
		seg, err := createSegment(l.cfg.Dir, l.next)
		if err != nil {
			return 0, err
		}
		l.segs = append(l.segs, seg)
		active = seg
	}
	if _, err := active.append(rec); err != nil {
		return 0, err
	}
	off := l.next
	l.next++
	l.inflight += int64(len(rec))
	if l.cfg.SyncOnAppend {
		if err := active.sync(); err != nil {
			return 0, err
		}
		l.inflight = 0
	}
	return off, nil
}

// Read 读取逻辑偏移 off 处的完整记录。
// 残余/损坏数据不会作为完整记录返回，错误原因可区分。
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
	if len(l.segs) > 0 && off < l.segs[0].base {
		l.mu.Unlock()
		return nil, ErrOffsetReclaimed
	}
	if off >= l.next {
		l.mu.Unlock()
		return nil, ErrOffsetOutOfRange
	}
	// 定位包含 off 的段：base <= off < base+count。
	idx := sort.Search(len(l.segs), func(i int) bool {
		s := l.segs[i]
		return off < s.base+s.count
	})
	if idx >= len(l.segs) || off < l.segs[idx].base {
		l.mu.Unlock()
		return nil, ErrOffsetOutOfRange
	}
	seg := l.segs[idx]
	l.mu.Unlock()

	// 段内顺序扫描至目标逻辑偏移，只返回校验通过的完整记录。
	logical := seg.base
	var pos int64
	for {
		hdr, err := seg.readAt(pos, recordHdrSize)
		if err != nil {
			return nil, &CorruptError{Kind: CorruptTornTail, Segment: filepath.Base(seg.path), Offset: logical, Detail: "short header read"}
		}
		length := uint32(hdr[4]) | uint32(hdr[5])<<8 | uint32(hdr[6])<<16 | uint32(hdr[7])<<24
		total := recordHdrSize + int(length)
		buf := make([]byte, total)
		copy(buf, hdr)
		rest, err := seg.readAt(pos+recordHdrSize, total-recordHdrSize)
		if err != nil {
			return nil, &CorruptError{Kind: CorruptTornTail, Segment: filepath.Base(seg.path), Offset: logical, Detail: "short payload read"}
		}
		copy(buf[recordHdrSize:], rest)
		payload, _, derr := decodeRecord(buf)
		if derr != nil {
			ce := derr.(*CorruptError)
			ce.Segment = filepath.Base(seg.path)
			ce.Offset = logical
			return nil, ce
		}
		if logical == off {
			out := make([]byte, len(payload))
			copy(out, payload)
			return out, nil
		}
		logical++
		pos += int64(total)
	}
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
// 只整体删除完全落在 before 之前的段，仍被读取位置引用的数据不会被回收。
func (l *Log) Reclaim(before uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	// 保留活跃段，避免破坏正在写入的位置。
	for len(l.segs) > 1 {
		s := l.segs[0]
		if s.base+s.count > before {
			break
		}
		if err := s.remove(); err != nil {
			return err
		}
		l.segs = l.segs[1:]
	}
	return nil
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

// Close 关闭日志，释放文件句柄。
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var errs []error
	for _, s := range l.segs {
		if err := s.close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
