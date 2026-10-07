package seglog

import (
	"os"
	"sync"
)

// handleCache 限制同时打开的底层文件句柄数（MaxOpenFiles，0 表示不限）。
// 段文件句柄懒打开：使用时 acquire，用完 release；超过上限时按 LRU 淘汰空闲句柄。
// 正在使用（pins>0）的句柄不会被淘汰；若上限内没有可淘汰的空闲句柄，
// acquire 返回 false，调用方将其归类为读侧限流（ErrTooManyReaders），而不是阻塞或超开。
//
// 锁序：handleCache.mu 是叶子锁，可在持有 Log.mu 时获取，反之不行。
type handleCache struct {
	mu   sync.Mutex
	cap  int        // 0 = 不限
	open int        // 当前打开的句柄数
	idle []*segment // 已打开但空闲的段，队首最久未使用
}

// acquire 为 seg 取得一个打开的文件句柄并钉住（pins++）。
func (c *handleCache) acquire(seg *segment) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seg.file != nil {
		seg.pins++
		c.touchLocked(seg)
		return true, nil
	}
	if c.cap > 0 && c.open >= c.cap {
		// 淘汰最久未使用的空闲句柄腾位置。
		for len(c.idle) > 0 {
			victim := c.idle[0]
			c.idle = c.idle[1:]
			if victim.pins > 0 || victim.file == nil {
				continue
			}
			if err := victim.close(); err != nil {
				return false, err
			}
			c.open--
			break
		}
		if c.open >= c.cap {
			return false, nil
		}
	}
	f, err := os.OpenFile(seg.path, os.O_RDWR, 0o644)
	if err != nil {
		return false, err
	}
	seg.file = f
	seg.pins = 1
	c.open++
	c.idle = append(c.idle, seg)
	return true, nil
}

// release 释放 acquire 取得的引用；句柄保持打开，留给缓存复用。
func (c *handleCache) release(seg *segment) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seg.pins > 0 {
		seg.pins--
	}
	if seg.pins == 0 && seg.file != nil {
		c.touchLocked(seg) // 确保在空闲列表中，可被淘汰
	}
}

// touchLocked 把 seg 移到空闲队尾（最近使用）。
func (c *handleCache) touchLocked(seg *segment) {
	for i, s := range c.idle {
		if s == seg {
			c.idle = append(c.idle[:i], c.idle[i+1:]...)
			break
		}
	}
	c.idle = append(c.idle, seg)
}

// evict 关闭并移除 seg 的句柄（段被回收或日志关闭时调用）。
func (c *handleCache) evict(seg *segment) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictLocked(seg)
}

func (c *handleCache) evictLocked(seg *segment) {
	for i, s := range c.idle {
		if s == seg {
			c.idle = append(c.idle[:i], c.idle[i+1:]...)
			break
		}
	}
	if seg.file != nil {
		seg.close()
		c.open--
	}
	seg.pins = 0
}

// adopt 把一个新创建（文件已打开）的段登记进缓存，必要时先淘汰空闲句柄。
// 上限内腾不出位置时返回 false（句柄不超开），调用方负责清理新段。
func (c *handleCache) adopt(seg *segment) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.cap > 0 && c.open >= c.cap && len(c.idle) > 0 {
		victim := c.idle[0]
		c.idle = c.idle[1:]
		if victim.pins > 0 || victim.file == nil {
			continue
		}
		if victim.close() == nil {
			c.open--
		}
	}
	if c.cap > 0 && c.open >= c.cap {
		return false
	}
	c.open++
	c.idle = append(c.idle, seg)
	return true
}

// pinned 报告 seg 的句柄当前是否被使用者钉住。
func (c *handleCache) pinned(seg *segment) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return seg.pins > 0
}
