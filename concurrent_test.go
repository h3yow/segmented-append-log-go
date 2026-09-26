package seglog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentWritersContiguous(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 128
	l, info, _ := Open(dir, cfg)
	t.Logf("[conc-writer] start committed=%d", info.CommittedPosition)

	const writers = 8
	const perWriter = 100
	var wg sync.WaitGroup
	var failures int64
	positions := make([][]uint64, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			seen := make([]uint64, 0, perWriter)
			for i := 0; i < perWriter; i++ {
				p, err := l.Append(context.Background(),
					[]byte(fmt.Sprintf("w%d-%04d", w, i)))
				if err != nil {
					atomic.AddInt64(&failures, 1)
					return
				}
				seen = append(seen, p)
			}
			positions[w] = seen
		}(w)
	}
	wg.Wait()
	if failures != 0 {
		t.Fatalf("%d writer failures", failures)
	}
	total := uint64(writers * perWriter)
	if l.CommittedPosition() != total {
		t.Fatalf("committed=%d want %d", l.CommittedPosition(), total)
	}

	// 所有位置 0..total-1 必须恰好被分配一次。
	seen := make([]bool, total)
	for _, ps := range positions {
		for _, p := range ps {
			if p >= total || seen[p] {
				t.Fatalf("duplicate/out-of-range position %d", p)
			}
			seen[p] = true
		}
	}
	for i, ok := range seen {
		if !ok {
			t.Fatalf("missing position %d", i)
		}
	}
	t.Logf("[conc-writer] %d writers x %d = %d unique contiguous positions",
		writers, perWriter, total)
	l.Close()
}

func TestConcurrentReadersNoLossNoDup(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 128
	l, _, _ := Open(dir, cfg)

	const writersN = 4
	const perW = 80
	const readersN = 5
	total := writersN * perW

	// 读取方全部从 0 开始，应各自读到全部记录、每条恰好一次。
	var wg sync.WaitGroup
	errCh := make(chan error, readersN)
	for r := 0; r < readersN; r++ {
		rd, err := l.NewReader(0)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(rd *Reader) {
			defer wg.Done()
			got := make([]bool, total)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for i := 0; i < total; i++ {
				pos, data, err := rd.Next(ctx)
				if err != nil {
					errCh <- err
					return
				}
				if pos >= uint64(total) || got[pos] {
					errCh <- fmt.Errorf("dup/oor pos=%d", pos)
					return
				}
				got[pos] = true
				if len(data) == 0 {
					errCh <- fmt.Errorf("empty data at %d", pos)
					return
				}
			}
			for i, ok := range got {
				if !ok {
					errCh <- fmt.Errorf("reader missed %d", i)
					return
				}
			}
			rd.Close()
		}(rd)
	}

	var pwg sync.WaitGroup
	for w := 0; w < writersN; w++ {
		pwg.Add(1)
		go func(w int) {
			defer pwg.Done()
			for i := 0; i < perW; i++ {
				if _, err := l.Append(context.Background(),
					[]byte(fmt.Sprintf("w%d-%04d", w, i))); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	pwg.Wait()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	t.Logf("[conc-reader] %d readers each saw all %d committed records exactly once",
		readersN, total)
	l.Close()
}

// 慢速消费者与持续写入并存：写入不因为有落后读取方而阻塞进程；
// 读取方以自己的节奏读到连续数据。
func TestSlowConsumerUnderContinuousWrites(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 64
	// 不限分段数/字节数，验证“持续写入不无限阻塞”，只验证消费连续性。
	l, _, _ := Open(dir, cfg)

	rd, err := l.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	const want = 60
	go func() {
		for i := 0; i < want; i++ {
			if _, err := l.Append(context.Background(),
				[]byte(fmt.Sprintf("slow-%04d-padding", i))); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := uint64(0); i < want; i++ {
		pos, _, err := rd.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if pos != i {
			t.Fatalf("slow consumer gap pos=%d want %d", pos, i)
		}
		time.Sleep(2 * time.Millisecond) // 故意慢于生产者
	}
	t.Logf("[slow-consumer] read %d contiguous records while writes continued", want)
	rd.Close()
	l.Close()
}

// 阻塞读取在日志关闭时必须可被唤醒并得到 ErrClosed，而不是永久挂住。
func TestNextWakesOnClose(t *testing.T) {
	dir := t.TempDir()
	l, _, _ := Open(dir, testConfig())
	rd, _ := l.NewReader(0)
	done := make(chan error, 1)
	go func() {
		_, _, err := rd.Next(context.Background())
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("want ErrClosed, got %v", err)
		}
		t.Logf("[wake-on-close] blocked Next returned: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Next did not wake on Close")
	}
}
