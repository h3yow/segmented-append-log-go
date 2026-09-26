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
	base  uint64 // 段内第一条记录的逻辑偏移
	path  string
	file  *os.File
	size  int64 // 已确认完整的字节数
	count uint64 // 段内记录数
}

// segmentFileName 由基础偏移生成段文件名。
func segmentFileName(base uint64) string {
	return fmt.Sprintf("%020d.seg", base)
}

// parseSegmentFileName 解析段文件名，返回基础偏移。
func parseSegmentFileName(name string) (uint64, bool) {
	if !strings.HasSuffix(name, ".seg") {
		return 0, false
	}
	base, err := strconv.ParseUint(strings.TrimSuffix(name, ".seg"), 10, 64)
	if err != nil {
		return 0, false
	}
	return base, true
}

// createSegment 创建新段文件。
func createSegment(dir string, base uint64) (*segment, error) {
	path := filepath.Join(dir, segmentFileName(base))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	return &segment{base: base, path: path, file: f}, nil
}

// openSegment 打开已有段文件。
func openSegment(dir string, base uint64) (*segment, error) {
	path := filepath.Join(dir, segmentFileName(base))
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &segment{base: base, path: path, file: f, size: st.Size()}, nil
}

// append 向段尾追加一条已编码记录，返回写入前段内偏移。
func (s *segment) append(rec []byte) (int64, error) {
	off := s.size
	n, err := s.file.WriteAt(rec, off)
	if err != nil {
		return 0, err
	}
	s.size += int64(n)
	s.count++
	return off, nil
}

// readAt 从段内指定字节偏移读取 n 字节。
func (s *segment) readAt(off int64, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := s.file.ReadAt(buf, off)
	if err != nil {
		return nil, err
	}
	return buf, nil
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

// close 关闭文件句柄。
func (s *segment) close() error { return s.file.Close() }

// remove 关闭并删除段文件。
func (s *segment) remove() error {
	if err := s.file.Close(); err != nil {
		return err
	}
	return os.Remove(s.path)
}
