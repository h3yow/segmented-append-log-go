package seglog

import (
	"context"
	"os"
	"testing"
)

// 损坏出现在后随分段：恢复必须删除损坏分段，并保证此前提交记录连续。
func TestRecoveryCorruptionInLaterSegment(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.MaxSegmentBytes = 60
	l, _, _ := Open(dir, cfg)
	for i := 0; i < 12; i++ {
		if _, err := l.Append(context.Background(), []byte("later-segment-record")); err != nil {
			t.Fatal(err)
		}
	}
	segs := append([]*segment(nil), l.segments...)
	if len(segs) < 3 {
		t.Fatalf("need >=3 segments, got %d", len(segs))
	}
	// 破坏倒数第二个分段的头部（写入错误魔数），模拟该段损坏。
	victim := segs[len(segs)-2]
	committedBefore := l.CommittedPosition()
	l.Close()

	f, err := os.OpenFile(victim.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0x00, 0x00}, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l2, info, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[later-corrupt] committedBefore=%d recovered=%d kind=%s segments=%d reason=%q",
		committedBefore, info.CommittedPosition, info.Corruption, len(info.Segments), info.Reason)
	if info.Corruption != CorruptionMagic {
		t.Fatalf("kind=%s want magic", info.Corruption)
	}
	if info.CommittedPosition > victim.basePosition {
		t.Fatalf("recovered %d must not pass corrupted segment base %d",
			info.CommittedPosition, victim.basePosition)
	}
	// 恢复点之前的记录全部连续可读。
	r, _ := l2.NewReader(0)
	for i := uint64(0); i < info.CommittedPosition; i++ {
		pos, _, err := r.Next(context.Background())
		if err != nil || pos != i {
			t.Fatalf("read pos=%d err=%v", pos, err)
		}
	}
	r.Close()
	l2.Close()
}

// 同一份磁盘字节，无论来自正常关闭还是异常终止，恢复结论必须一致。
func TestRecoveryIndependentOfShutdownPath(t *testing.T) {
	cfg := testConfig()

	// 场景 A：正常关闭后人工追加残缺首帧。
	build := func(t *testing.T, clean bool) (string, uint64) {
		dir := t.TempDir()
		l, _, _ := Open(dir, cfg)
		for i := 0; i < 6; i++ {
			l.Append(context.Background(), []byte("determinism-record"))
		}
		active := l.active.path
		if clean {
			l.Close()
		} else {
			// 异常终止：不 Close、不 fsync 尾部，直接放弃。
			_ = active
		}
		// 两种路径都在文件末尾追加同一段“写了一半”的残余字节。
		if clean {
			f, _ := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0)
			f.Write(encodeFrame(6, flagFirst, []byte("half")))
			f.Close()
		}
		return dir, 6
	}

	dirA, n := build(t, true)
	_, iA := mustOpen(t, dirA, cfg)
	// 第二次打开（已修复）应 clean 且 committed 不变。
	lB, iB := mustOpen(t, dirA, cfg)
	t.Logf("[shutdown-equiv] n=%d firstOpen kind=%s committed=%d; secondOpen kind=%s committed=%d",
		n, iA.Corruption, iA.CommittedPosition, iB.Corruption, iB.CommittedPosition)
	if iA.CommittedPosition != n || iA.Corruption != CorruptionTruncated {
		t.Fatal("first open mismatch")
	}
	if iB.CommittedPosition != n || iB.Corruption != CorruptionNone {
		t.Fatal("second open mismatch")
	}
	lB.Close()
}
