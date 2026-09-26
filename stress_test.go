package seglog

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 持续写入 + 多个追平即关闭的读取方 + 后台回收，跑 race 验证无死锁、
// 无数据竞争，且总提交位置最终等于写入条数。
func TestStressWriteReadReclaim(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 96
	l, _, _ := Open(dir, cfg)

	const writers = 4
	const per = 200
	total := uint64(writers * per)
	var wg sync.WaitGroup

	stop := make(chan struct{})
	var reclaimed int64
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if n, _, err := l.Reclaim(); err == nil {
				atomic.AddInt64(&reclaimed, int64(n))
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// 读取方不断创建并读到末端后关闭，制造回收机会。
	var rwg sync.WaitGroup
	rstop := make(chan struct{})
	// 快速追平读取方：反复从最早位置读到当前末端，读完即关闭，
	// 使后台 Reclaim 能真正回收历史分段。
	rwg.Add(1)
	go func() {
		defer rwg.Done()
		for {
			select {
			case <-rstop:
				return
			default:
			}
			fp := l.FirstPosition()
			tip := l.CommittedPosition()
			if tip <= fp {
				time.Sleep(time.Millisecond)
				continue
			}
			rd, err := l.NewReader(fp)
			if err != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			expect := fp
			for {
				p, _, err := rd.TryNext()
				if err != nil {
					break
				}
				if p != expect {
					t.Errorf("fast gap p=%d expect=%d", p, expect)
				}
				expect++
				if p+1 >= l.CommittedPosition() {
					break
				}
			}
			rd.Close()
		}
	}()
	for r := 0; r < 3; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			var pos uint64
			var rd *Reader
			for {
				select {
				case <-rstop:
					if rd != nil {
						rd.Close()
					}
					return
				default:
				}
				if rd == nil {
					var err error
					rd, err = l.NewReader(l.FirstPosition())
					if err != nil {
						time.Sleep(time.Millisecond)
						continue
					}
					pos = l.FirstPosition()
				}
				p, _, err := rd.TryNext()
				switch {
				case err == nil:
					if p != pos {
						t.Errorf("gap p=%d pos=%d", p, pos)
						return
					}
					pos++
					if pos >= total {
						rd.Close()
						rd = nil
					}
				default:
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, err := l.Append(context.Background(),
					[]byte("stress-record-payload")); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(rstop)
	rwg.Wait()
	close(stop)

	if l.CommittedPosition() != total {
		t.Fatalf("committed=%d want %d", l.CommittedPosition(), total)
	}
	t.Logf("[stress] committed=%d segmentsReclaimed=%d remainingSegs=%d",
		l.CommittedPosition(), atomic.LoadInt64(&reclaimed), len(l.segments))
	l.Close()
}
