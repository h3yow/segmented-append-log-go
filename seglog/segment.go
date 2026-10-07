package seglog

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// segment 对应磁盘上一个段文件。
type segment struct {
	base      uint64 // 段内第一条记录的逻辑偏移（续段：被续写的记录偏移）
	part      uint16 // 分片序号：0 为普通段，>0 为跨段记录的第 N 个续段
	continued bool   // 本段以某条记录的非首分片开头（首分片在之前的段里）
	path      string
	file      *os.File // 懒打开，可能为 nil（由句柄缓存按需打开）
	size      int64    // 已确认完整的字节数
	count     uint64   // 在本段中起始的记录数
	pins      int      // 句柄缓存：当前使用者数，>0 时不可被淘汰
}

// end 返回本段覆盖的逻辑偏移终点（不含）。
// 续段的开头字节属于一条在之前的段里起始的记录，因此覆盖率 +1。
func (s *segment) end() uint64 {
	e := s.base + s.count
	if s.continued {
		e++
	}
	return e
}

// segmentFileName 由基础偏移与分片序号生成段文件名。
func segmentFileName(base uint64, part uint16) string {
	if part == 0 {
		return fmt.Sprintf("%020d.seg", base)
	}
	return fmt.Sprintf("%020d-%04d.seg", base, part)
}

// parseSegmentFileName 解析段文件名，返回基础偏移与分片序号。
func parseSegmentFileName(name string) (base uint64, part uint16, ok bool) {
	if !strings.HasSuffix(name, ".seg") {
		return 0, 0, false
	}
	stem := strings.TrimSuffix(name, ".seg")
	num := stem
	if i := strings.LastIndexByte(stem, '-'); i >= 0 {
		p, err := strconv.ParseUint(stem[i+1:], 10, 16)
		if err != nil || p == 0 {
			return 0, 0, false
		}
		part = uint16(p)
		num = stem[:i]
	}
	base, err := strconv.ParseUint(num, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return base, part, true
}

// createSegment 创建新段文件（文件立即打开，供写入）。
func createSegment(dir string, base uint64, part uint16) (*segment, error) {
	path := filepath.Join(dir, segmentFileName(base, part))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	return &segment{base: base, part: part, continued: part > 0, path: path, file: f}, nil
}

// openSegment 登记已有段文件；文件句柄懒打开（由句柄缓存按需打开）。
func openSegment(dir string, base uint64, part uint16) (*segment, error) {
	path := filepath.Join(dir, segmentFileName(base, part))
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return &segment{base: base, part: part, continued: part > 0, path: path, size: st.Size()}, nil
}

// append 向段尾追加一条已编码记录，返回写入前段内偏移。
func (s *segment) append(rec []byte) (int64, error) {
	off, err := s.appendRaw(rec)
	if err != nil {
		return 0, err
	}
	s.count++
	return off, nil
}

// appendRaw 向段尾追加字节但不增加记录计数（用于跨段记录的分片：
// 分片不是独立记录，完整记录只在首分片所在段计一次）。
func (s *segment) appendRaw(p []byte) (int64, error) {
	off := s.size
	n, err := s.file.WriteAt(p, off)
	if err != nil {
		return 0, err
	}
	s.size += int64(n)
	return off, nil
}

// truncateTo 将段截断到指定字节数（用于恢复时切除尾部残余）。
func (s *segment) truncateTo(size int64) error {
	if err := s.file.Truncate(size); err != nil {
		return err
	}
	s.size = size
	return nil
}

// sync 刷盘。
func (s *segment) sync() error { return s.file.Sync() }

// close 关闭文件句柄（幂等，句柄未打开时为空操作）。
func (s *segment) close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

// remove 关闭并删除段文件。
func (s *segment) remove() error {
	if err := s.close(); err != nil {
		return err
	}
	return os.Remove(s.path)
}
