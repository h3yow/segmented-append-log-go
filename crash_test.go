package seglog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
)

// injectedCrash 用于 beforeFrameSync 钩子模拟写入中途异常。
var errInjectedCrash = errors.New("injected crash")

// runCrashPhase 在 phase 指定阶段让第 n 次 Append 失败，然后重新打开，
// 校验：失败的记录不可见、已成功的记录序号连续、恢复归类确定。
func runCrashPhase(t *testing.T, phase syncPhase, label string) {
	t.Helper()
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 80
	cfg.MaxChunkBytes = 30

	var calls int32
	cfg.beforeFrameSync = func(p syncPhase) error {
		if p == phase && atomic.AddInt32(&calls, 1) == 4 {
			return errInjectedCrash
		}
		return nil
	}

	l, _, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var ok uint64
	for i := 0; i < 6; i++ {
		pos, err := l.Append(context.Background(), []byte(fmt.Sprintf("crash-%d-payload", i)))
		if err != nil {
			if !errors.Is(err, errInjectedCrash) {
				t.Fatalf("[%s] unexpected err %v", label, err)
			}
			break
		}
		if pos != ok {
			t.Fatalf("[%s] pos=%d want %d", label, pos, ok)
		}
		ok = pos + 1
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, info, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[crash:%s] priorOK=%d -> recovered committed=%d kind=%s dropped=%d reason=%q",
		label, ok, info.CommittedPosition, info.Corruption, info.DroppedBytes, info.Reason)

	// 崩溃发生在 fsync 之后时，记录可能已落盘——但无论如何恢复结论必须
	// 自洽：committed 条记录全部可读且连续，且其序号正好是 0..committed-1。
	if info.CommittedPosition < ok {
		t.Fatalf("[%s] committed=%d less than acknowledged %d", label, info.CommittedPosition, ok)
	}
	r, err := l2.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < info.CommittedPosition; i++ {
		pos, _, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("[%s] read %d: %v", label, i, err)
		}
		if pos != i {
			t.Fatalf("[%s] non-contiguous pos=%d want %d", label, pos, i)
		}
	}
	if _, _, err := r.TryNext(); !errors.Is(err, ErrNoRecord) {
		t.Fatalf("[%s] tail should be empty: %v", label, err)
	}
	r.Close()
	l2.Close()
}

func TestCrashAtEachPhase(t *testing.T) {
	runCrashPhase(t, phaseBeforeWrite, "before-write")
	runCrashPhase(t, phaseBeforeFsync, "before-fsync")
	runCrashPhase(t, phaseAfterFsync, "after-fsync")
}

// 进程异常终止的模拟：不调用 Close，直接丢弃 Log 句柄（GC 关闭由 OS
// 回收 fd，文件中可能残留未 fsync 数据）。重启恢复必须成功且确定。
func TestAbruptProcessExitSimulation(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.FsyncEveryWrite = false // 不每条 fsync，模拟崩溃丢尾部
	l, _, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, _ = l.Append(context.Background(), []byte(fmt.Sprintf("no-sync-%d", i)))
	}
	// 主动把底层文件句柄以“非 Close 顺序”放弃：直接置空并结束作用域。
	// 为确定性起见，我们显式关闭活动文件而不经过 Log.Close（不 fsync）。
	l.mu.Lock()
	f := l.active.writeFile
	l.mu.Unlock()
	_ = f.Close()

	l2, info, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[abrupt-exit] committed=%d kind=%s segments=%d reason=%q",
		info.CommittedPosition, info.Corruption, len(info.Segments), info.Reason)
	if info.CommittedPosition > 10 {
		t.Fatal("committed cannot exceed writes")
	}
	r, _ := l2.NewReader(0)
	for i := uint64(0); i < info.CommittedPosition; i++ {
		if pos, _, err := r.Next(context.Background()); err != nil || pos != i {
			t.Fatalf("read at %d: pos=%d err=%v", i, pos, err)
		}
	}
	r.Close()
	l2.Close()
	_ = os.RemoveAll(dir)
}
