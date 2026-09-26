package seglog

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// reader.go：独立游标的读取方。读取方只能看到完整且已提交的记录，
// 且逻辑位置严格连续推进，不重复、不跳号。

// Reader 是一个顺序读取游标，多个 Reader 相互独立。
//
// 不变量：正常写入路径只在“记录首帧之前”滚动分段，因此每个分段的
// 第一帧必为某条记录的首帧（flagFirst），且一条记录的全部分片都
// 落在同一分段内。游标维护 (segIdx, segOff) 指向 下一条记录首帧。
type Reader struct {
	log *Log
	mu  sync.Mutex
	pos atomic.Uint64 // 下一条待读取记录序号（回收方会在其自身锁外读取）

	segIdx int   // 下一记录首帧所在分段下标
	segOff int64 // 下一记录首帧在该分段内的偏移
	closed bool

	// wake 在 Reader 关闭时收到信号。
	wake chan struct{}
}

// Next 阻塞等待并返回下一条已提交记录。返回的 position 是该记录的
// 逻辑位置（记录序号）。数据拷贝归调用方所有。
func (r *Reader) Next(ctx context.Context) (uint64, []byte, error) {
	for {
		pos, data, err := r.readOnce()
		if err == nil {
			return pos, data, nil
		}
		if !errors.Is(err, ErrNoRecord) {
			return 0, nil, err
		}
		// 等待提交推进或关闭。
		r.log.mu.Lock()
		if r.log.closed {
			r.log.mu.Unlock()
			return 0, nil, ErrClosed
		}
		cp := r.log.committedPosition
		notify := r.log.notify
		r.log.mu.Unlock()
		if cp > r.snapshotPos() {
			continue
		}

		r.mu.Lock()
		wake := r.wake
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return 0, nil, ErrClosed
		}

		select {
		case <-notify:
		case <-wake:
			return 0, nil, ErrClosed
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
}

// TryNext 非阻塞尝试读取；没有已提交新记录时返回 ErrNoRecord。
func (r *Reader) TryNext() (uint64, []byte, error) {
	return r.readOnce()
}

// Position 返回下一条待读取记录的逻辑位置。
func (r *Reader) Position() uint64 { return r.pos.Load() }

// Close 释放读取方，其位置不再阻止旧分段回收。可重复调用。
func (r *Reader) Close() error {
	r.log.unregisterReader(r)
	return nil
}

func (r *Reader) snapshotPos() uint64 { return r.pos.Load() }

// markClosed 由 Log.Close 调用，唤醒阻塞中的读取。
func (r *Reader) markClosed() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		close(r.wake)
	}
}

// readOnce 执行一次非阻塞读取尝试。
func (r *Reader) readOnce() (uint64, []byte, error) {
	r.log.mu.Lock()
	if r.log.closed {
		r.log.mu.Unlock()
		return 0, nil, ErrClosed
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.log.mu.Unlock()
		return 0, nil, ErrClosed
	}
	pos := r.pos.Load()
	idx := r.segIdx
	off := r.segOff
	r.mu.Unlock()

	if pos < r.log.firstPosition {
		r.log.mu.Unlock()
		return 0, nil, ErrPositionReclaimed
	}
	if pos >= r.log.committedPosition {
		r.log.mu.Unlock()
		return 0, nil, ErrNoRecord
	}
	segments := r.log.segments
	cache := r.log.cache
	// 可读边界：活动分段下标与其“已提交字节长度”。读取不得越过该长度，
	// 否则会 pread 到写入方正在写、尚未提交的字节（既可能读到半条记录，
	// 也会与写入形成数据竞争）。该长度在 l.mu 下读取，与写路径更新
	// seg.size 互斥，因此快照内的全部字节都是已提交的。
	limitIdx := len(segments) - 1
	limitOff := segments[limitIdx].size
	// 游标可能停在已被读尽分段的边界：跳到下一分段头。
	if idx >= len(segments) || (idx < limitIdx && off >= segments[idx].size) ||
		(idx == limitIdx && off >= limitOff) {
		idx++
		off = 0
	}
	if idx > limitIdx || (idx == limitIdx && off >= limitOff) {
		r.log.mu.Unlock()
		return 0, nil, ErrNoRecord
	}
	r.log.mu.Unlock()

	data, nextIdx, nextOff, err := readRecordAt(segments, cache, idx, off, pos, limitIdx, limitOff)
	if err != nil {
		return 0, nil, err
	}
	r.pos.Store(pos + 1)
	r.mu.Lock()
	r.segIdx = nextIdx
	r.segOff = nextOff
	r.mu.Unlock()
	return pos, data, nil
}

// readRecordAt 从 segments[idx] 的 off 偏移（必须是记录首帧）开始读取
// 序号 seq 的记录，拼接同分段后续分片，返回完整负载以及下一条记录的
// (分段下标, 帧偏移)。调用保证 seq 已提交；任何底层不一致都映射为
// 可区分错误而不是返回半条记录。
func readRecordAt(segments []*segment, cache *fileCache, idx int, off int64, seq uint64, limitIdx int, limitOff int64) ([]byte, int, int64, error) {
	if idx >= len(segments) {
		return nil, idx, off, ErrBadPosition
	}
	// readableEnd 返回某分段允许读取的字节末端（排他）。
	readableEnd := func(i int) int64 {
		if i == limitIdx {
			return limitOff
		}
		return segments[i].size
	}
	var out []byte
	chainOpen := false
	for i := idx; i <= limitIdx; i++ {
		seg := segments[i]
		end := readableEnd(i)
		for off < end {
			h, payload, herr := readFrameBounded(seg, cache, off, end)
			if herr != nil {
				if errors.Is(herr, errFrameTruncated) {
					return nil, i, off, ErrIncompleteRecord
				}
				return nil, i, off, herr
			}
			if !h.headerOK() {
				return nil, i, off, ErrInvalidMagic
			}
			if crc32Chunk(payload) != h.dataCRC {
				return nil, i, off, ErrChecksumMismatch
			}
			flags := h.flags & (flagFirst | flagLast)
			nextOff := off + frameHeaderSize + int64(h.dataLen)
			switch flags {
			case flagFirst | flagLast:
				if chainOpen || h.seq != seq {
					return nil, i, off, ErrSequenceGap
				}
				out = append(out, payload...)
				return out, i, nextOff, nil
			case flagFirst:
				if chainOpen || h.seq != seq {
					return nil, i, off, ErrSequenceGap
				}
				chainOpen = true
				out = append(out, payload...)
			case 0:
				if !chainOpen || h.seq != seq {
					return nil, i, off, ErrSequenceGap
				}
				out = append(out, payload...)
			case flagLast:
				if !chainOpen || h.seq != seq {
					return nil, i, off, ErrSequenceGap
				}
				out = append(out, payload...)
				if nextOff >= end {
					return out, i + 1, 0, nil
				}
				return out, i, nextOff, nil
			}
			off = nextOff
		}
		// 到达本分段可读末端，延续到下一分段头。
		off = 0
	}
	return nil, idx, off, ErrIncompleteRecord
}
