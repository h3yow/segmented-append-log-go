package seglog

import (
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// segment.go：单个分段文件的内存表示与有界句柄管理。

const segmentPrefix = "segment-"
const segmentSuffix = ".log"

// segment 表示一个分段文件。basePosition 为该分段第一条记录的逻辑序号；
// startOffset 为该文件在全局字节流中的起始偏移（回收时用于安全判定）。
type segment struct {
	path         string
	basePosition uint64
	startOffset  int64
	size         int64

	// writeFile 是写路径专用的读写句柄，仅活动分段持有；滚动后关闭。
	// 读取方绝不使用该句柄，从而与写入并发隔离。
	writeFile *os.File

	// readFile 是读路径专用的只读句柄（独立 fd，独立文件偏移），
	// 由 fileCache 按 LRU 换入换出，保证文件句柄数量有界。
	readFile *os.File

	// refs 为正在读取该分段的读取方数量；被引用的分段不会被换出/删除。
	refs int

	// lruEl 是该分段在 LRU 链表中的元素。
	lruEl *list.Element

	// rwMu 建立“写一条记录”与“读该分段帧”之间的 happens-before 关系：
	// 追加方在整条记录的全部帧写入窗口内持有写锁；读取方读取任一帧时
	// 持有读锁。已密封（不再写入）的分段永远不会有写锁竞争，因此该锁
	// 只在活动分段上产生作用，不影响历史数据的并发读吞吐。
	rwMu sync.RWMutex
}

// segmentName 生成分段文件名（20 位零填充序号，保证字典序即位置序）。
func segmentName(basePosition uint64) string {
	return fmt.Sprintf("%s%020d%s", segmentPrefix, basePosition, segmentSuffix)
}

// parseSegmentName 从文件名解析 basePosition；不匹配时返回 false。
func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	mid := name[len(segmentPrefix) : len(name)-len(segmentSuffix)]
	n, err := strconv.ParseUint(mid, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// createSegment 在 dir 下创建新分段并同步目录项，保证崩溃后文件可见。
func createSegment(dir string, basePosition uint64, startOffset int64) (*segment, error) {
	path := filepath.Join(dir, segmentName(basePosition))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &segment{
		path:         path,
		basePosition: basePosition,
		startOffset:  startOffset,
		writeFile:    f,
	}, nil
}

// openRO 以只读方式打开分段（供 fileCache 换入使用）。使用独立 fd，
// 与写句柄互不影响。
func (s *segment) openRO() error {
	if s.readFile != nil {
		return nil
	}
	f, err := os.OpenFile(s.path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	s.readFile = f
	return nil
}

// closeWrite 关闭写句柄（滚动后旧活动分段转为纯只读）。
func (s *segment) closeWrite() error {
	var err error
	if s.writeFile != nil {
		err = s.writeFile.Close()
		s.writeFile = nil
	}
	return err
}

// closeHandle 关闭只读句柄（不改变其它元数据）。
func (s *segment) closeHandle() error {
	var err error
	if s.readFile != nil {
		err = s.readFile.Close()
		s.readFile = nil
	}
	return err
}

// closeAllHandles 关闭写句柄与只读句柄。
func (s *segment) closeAllHandles() error {
	err1 := s.closeHandle()
	err2 := s.closeWrite()
	if err1 != nil {
		return err1
	}
	return err2
}

// syncDir 对目录执行 fsync，确保新建/删除/重命名的目录项落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	_ = d.Close()
	return err
}

// fileCache 以 LRU 方式管理读路径的只读句柄，保证 fd 数量有界。
// 写句柄（活动分段的 writeFile）不经过本缓存，与读句柄完全独立，
// 因而读写可在活动分段上并发且互不共享 *os.File。
//
// fd 预算：maxOpen = 1（活动分段写句柄）+ 只读句柄数。
type fileCache struct {
	maxOpen int // 总 fd 上限（含 1 个写句柄）

	mu     sync.Mutex
	lru    *list.List // *segment，最近使用在前端
	bySeg  map[*segment]*list.Element
	openRO int // 当前已打开的只读句柄数量
}

// newFileCache 创建句柄缓存。maxOpen 为总 fd 上限（>=2，留 1 给写句柄）。
func newFileCache(maxOpen int) *fileCache {
	if maxOpen < 2 {
		maxOpen = defaultMaxOpenFiles
	}
	return &fileCache{
		maxOpen: maxOpen,
		lru:     list.New(),
		bySeg:   make(map[*segment]*list.Element),
	}
}

// roBudget 返回只读句柄的最大数量（始终为活动写句柄预留 1 个）。
func (c *fileCache) roBudget() int {
	b := c.maxOpen - 1
	if b < 1 {
		b = 1
	}
	return b
}

// pin 确保分段只读句柄已打开并加引用；调用期间句柄不会被换出/删除。
// 若只读额度都被长期引用（refs>0）的分段占满，返回 ErrStorageLimit，
// 由上层表达为可区分的资源背压，而不是静默阻塞。
//
// 打开文件的动作在缓存锁内完成：fd 打开很快，且这样可避免两个 goroutine
// 同时为同一分段打开并竞争写入 readFile 字段。实际 ReadAt 发生在 pin
// 返回之后（不持缓存锁），不会阻塞其它分段的换入换出。
func (c *fileCache) pin(s *segment) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.bySeg[s]; ok {
		c.lru.MoveToFront(el)
		s.refs++
		return nil
	}
	if !c.reserveRoomLocked() {
		return ErrStorageLimit
	}
	if s.readFile == nil {
		if err := s.openRO(); err != nil {
			return err
		}
	}
	s.refs++
	s.lruEl = c.lru.PushFront(s)
	c.bySeg[s] = s.lruEl
	c.openRO++
	return nil
}

// reserveRoomLocked 必要时淘汰尾部未引用只读句柄，为 1 个新句柄腾额度。
func (c *fileCache) reserveRoomLocked() bool {
	for c.openRO+1 > c.roBudget() {
		victim := c.pickVictimLocked()
		if victim == nil {
			return false
		}
		c.lru.Remove(victim.lruEl)
		delete(c.bySeg, victim)
		victim.lruEl = nil
		c.openRO--
		_ = victim.closeHandle()
	}
	return true
}

// unpin 释放一次引用。
func (c *fileCache) unpin(s *segment) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.refs > 0 {
		s.refs--
	}
}

// pickVictimLocked 从尾部挑选一个 refs==0 且只读句柄已打开的分段。
func (c *fileCache) pickVictimLocked() *segment {
	for el := c.lru.Back(); el != nil; el = el.Prev() {
		s := el.Value.(*segment)
		if s.refs == 0 && s.readFile != nil {
			return s
		}
	}
	return nil
}

// remove 把分段彻底移出缓存（删除文件前调用）；若仍被引用则返回错误。
func (c *fileCache) remove(s *segment) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.refs > 0 {
		return ErrClosed
	}
	if el, ok := c.bySeg[s]; ok {
		c.lru.Remove(el)
		delete(c.bySeg, s)
		s.lruEl = nil
		c.openRO--
	}
	return s.closeAllHandles()
}

// closeAll 关闭给定全部分段的只读句柄（写句柄由调用方单独处理）。
func (c *fileCache) closeAll(segs []*segment) {
	c.mu.Lock()
	c.lru.Init()
	c.bySeg = make(map[*segment]*list.Element)
	c.openRO = 0
	c.mu.Unlock()
	for _, s := range segs {
		s.lruEl = nil
		_ = s.closeHandle()
	}
}

// openWrite 以读写方式打开活动分段（恢复装载时其写句柄可能尚未打开）。
func (s *segment) openWrite() error {
	if s.writeFile != nil {
		return nil
	}
	f, err := os.OpenFile(s.path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	s.writeFile = f
	return nil
}
