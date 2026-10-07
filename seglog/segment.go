package seglog

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// segment 对应磁盘上一个段文件。
// 文件句柄不常驻：所有读写经由 filePool 按需打开，池中句柄数受 MaxOpenFiles 限制。
type segment struct {
	base  uint64 // 逻辑 base：此段之前已开始的记录数
	seq   int    // 同一逻辑 base 下的序号（跨多段的纯续块段 base 相同，用 seq 区分）
	path  string
	size  int64  // 已确认完整的字节数
	count uint64 // 段内记录起始数（跨段记录只在其首块所在段计一次）
	pool  *filePool
}

// filePool 有上限的段文件句柄池：超出容量时按 LRU 关闭最久未用的句柄。
type filePool struct {
	mu   sync.Mutex
	cap  int
	open map[*segment]*os.File
	lru  []*segment // 旧 -> 新
}

func newFilePool(cap int) *filePool {
	if cap < 1 {
		cap = 1
	}
	return &filePool{cap: cap, open: make(map[*segment]*os.File)}
}

// withFile 取段文件句柄并执行 fn；句柄获取与淘汰在池锁内完成，
// 保证 fn 执行期间句柄不会被淘汰关闭。
func (p *filePool) withFile(s *segment, fn func(f *os.File) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.open[s]
	if !ok {
		if len(p.open) >= p.cap {
			p.evictOldestLocked()
		}
		nf, err := os.OpenFile(s.path, os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		f = nf
		p.open[s] = f
	}
	p.touchLocked(s)
	return fn(f)
}

func (p *filePool) touchLocked(s *segment) {
	for i, x := range p.lru {
		if x == s {
			p.lru = append(p.lru[:i], p.lru[i+1:]...)
			break
		}
	}
	p.lru = append(p.lru, s)
}

func (p *filePool) evictOldestLocked() {
	oldest := p.lru[0]
	p.lru = p.lru[1:]
	p.open[oldest].Close()
	delete(p.open, oldest)
}

// closeSegment 关闭并移除某段的句柄（段被删除或日志关闭时调用）。
func (p *filePool) closeSegment(s *segment) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if f, ok := p.open[s]; ok {
		f.Close()
		delete(p.open, s)
	}
	for i, x := range p.lru {
		if x == s {
			p.lru = append(p.lru[:i], p.lru[i+1:]...)
			break
		}
	}
}

// closeAll 关闭池中全部句柄。
func (p *filePool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for s, f := range p.open {
		f.Close()
		delete(p.open, s)
	}
	p.lru = nil
}

// openCount 返回当前池中句柄数（测试用）。
func (p *filePool) openCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.open)
}

// segmentFileName 由逻辑 base 与序号生成段文件名。
// 序号用于区分同一逻辑 base 的多个纯续块段（大记录跨 3 段以上时出现）。
func segmentFileName(base uint64, seq int) string {
	return fmt.Sprintf("%020d.%06d.seg", base, seq)
}

// parseSegmentFileName 解析段文件名，返回逻辑 base 与序号。
// 兼容无序号的旧格式 "%020d.seg"（序号视为 0）。
func parseSegmentFileName(name string) (base uint64, seq int, ok bool) {
	if !strings.HasSuffix(name, ".seg") {
		return 0, 0, false
	}
	stem := strings.TrimSuffix(name, ".seg")
	parts := strings.Split(stem, ".")
	if len(parts) > 2 {
		return 0, 0, false
	}
	base, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if len(parts) == 2 {
		s, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return 0, 0, false
		}
		seq = int(s)
	}
	return base, seq, true
}

// createSegment 创建新段文件（立即落盘创建，保证后续打开不需要 O_CREATE，
// 已被回收删除的段不会被误重建）。
func createSegment(pool *filePool, dir string, base uint64, seq int) (*segment, error) {
	path := filepath.Join(dir, segmentFileName(base, seq))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	return &segment{base: base, seq: seq, path: path, pool: pool}, nil
}

// openSegment 打开已有段（读取其当前大小，不常驻句柄）。
func openSegment(pool *filePool, dir string, base uint64, seq int) (*segment, error) {
	path := filepath.Join(dir, segmentFileName(base, seq))
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return &segment{base: base, seq: seq, path: path, size: st.Size(), pool: pool}, nil
}

// append 向段尾追加一条已编码记录/块，返回写入前段内偏移。
func (s *segment) append(rec []byte) (int64, error) {
	off := s.size
	err := s.pool.withFile(s, func(f *os.File) error {
		n, err := f.WriteAt(rec, off)
		if err != nil {
			return err
		}
		if n != len(rec) {
			return fmt.Errorf("seglog: short write %d/%d", n, len(rec))
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	s.size += int64(len(rec))
	return off, nil
}

// readAt 从段内指定字节偏移读取 n 字节。
func (s *segment) readAt(off int64, n int) ([]byte, error) {
	buf := make([]byte, n)
	err := s.pool.withFile(s, func(f *os.File) error {
		_, err := f.ReadAt(buf, off)
		return err
	})
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// truncateTo 将段截断到指定字节数（用于恢复时切除尾部残余）。
func (s *segment) truncateTo(size int64) error {
	err := s.pool.withFile(s, func(f *os.File) error {
		return f.Truncate(size)
	})
	if err != nil {
		return err
	}
	s.size = size
	return nil
}

// sync 刷盘。
func (s *segment) sync() error {
	return s.pool.withFile(s, func(f *os.File) error {
		return f.Sync()
	})
}

// close 关闭段句柄（从池中移除）。
func (s *segment) close() error {
	s.pool.closeSegment(s)
	return nil
}

// remove 关闭并删除段文件。
func (s *segment) remove() error {
	s.pool.closeSegment(s)
	return os.Remove(s.path)
}
