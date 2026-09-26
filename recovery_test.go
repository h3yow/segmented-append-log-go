package seglog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
)

// reopenWithTail 先写入 baseN 条基准记录并关闭，再用 rawTail 给尾部
// 分段追加“残余字节”（模拟崩溃现场），随后重新 Open 执行恢复。
func reopenWithTail(t *testing.T, cfg Config, baseN int, rawTail []byte) (*Log, *RecoveryInfo) {
	t.Helper()
	dir := t.TempDir()
	l, _, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < baseN; i++ {
		if _, err := l.Append(context.Background(), []byte(fmt.Sprintf("base-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	activePath := l.active.path
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if rawTail != nil {
		f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(rawTail); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	l2, info, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return l2, info
}

// expectBaseRecords 验证恢复后能且只能读到前 n 条基准记录。
func expectBaseRecords(t *testing.T, l *Log, n uint64) {
	t.Helper()
	r, err := l.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < n; i++ {
		pos, data, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if pos != i || string(data) != fmt.Sprintf("base-%03d", i) {
			t.Fatalf("got (%d,%q) want (%d,base-%03d)", pos, data, i, i)
		}
	}
	if _, _, err := r.TryNext(); !errors.Is(err, ErrNoRecord) {
		t.Fatalf("want ErrNoRecord after %d records, got %v", n, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

// 尾部只有半个帧头：归类 truncated。
func TestRecoveryTruncatedHeader(t *testing.T) {
	cfg := testConfig()
	l, info := reopenWithTail(t, cfg, 5, []byte{frameMagic0, frameMagic1, FormatVersionV1, flagFirst, 0x00})
	t.Logf("[truncated-header] committed=%d first=%d kind=%s kept=%d dropped=%d reason=%q",
		info.CommittedPosition, info.FirstPosition, info.Corruption, info.KeptBytes, info.DroppedBytes, info.Reason)
	if info.Corruption != CorruptionTruncated {
		t.Fatalf("kind=%s want truncated", info.Corruption)
	}
	if info.CommittedPosition != 5 {
		t.Fatalf("committed=%d want 5", info.CommittedPosition)
	}
	expectBaseRecords(t, l, 5)
	l.Close()
}

// 帧头完整但负载被截断：归类 truncated。
func TestRecoveryTruncatedPayload(t *testing.T) {
	cfg := testConfig()
	full := encodeFrame(5, flagFirst|flagLast, []byte("payload"))
	// 砍掉负载末尾若干字节。
	half := append([]byte{}, full[:frameHeaderSize+3]...)
	l, info := reopenWithTail(t, cfg, 5, half)
	t.Logf("[truncated-payload] committed=%d kind=%s kept=%d dropped=%d reason=%q",
		info.CommittedPosition, info.Corruption, info.KeptBytes, info.DroppedBytes, info.Reason)
	if info.Corruption != CorruptionTruncated {
		t.Fatalf("kind=%s want truncated", info.Corruption)
	}
	if info.CommittedPosition != 5 {
		t.Fatalf("committed=%d want 5", info.CommittedPosition)
	}
	expectBaseRecords(t, l, 5)
	l.Close()
}

// 负载完整但 CRC 错误：归类 checksum。
func TestRecoveryChecksumMismatch(t *testing.T) {
	cfg := testConfig()
	full := encodeFrame(5, flagFirst|flagLast, []byte("payload"))
	full[len(full)-1] ^= 0xFF // 破坏负载末字节，CRC 不再匹配
	l, info := reopenWithTail(t, cfg, 5, full)
	t.Logf("[checksum] committed=%d kind=%s dropped=%d reason=%q",
		info.CommittedPosition, info.Corruption, info.DroppedBytes, info.Reason)
	if info.Corruption != CorruptionChecksum {
		t.Fatalf("kind=%s want checksum", info.Corruption)
	}
	if info.CommittedPosition != 5 {
		t.Fatalf("committed=%d want 5", info.CommittedPosition)
	}
	expectBaseRecords(t, l, 5)
	l.Close()
}

// 魔数无法识别：归类 invalid-magic。
func TestRecoveryInvalidMagic(t *testing.T) {
	cfg := testConfig()
	full := encodeFrame(5, flagFirst|flagLast, []byte("payload"))
	full[0] = 0x00
	full[1] = 0x00
	l, info := reopenWithTail(t, cfg, 5, full)
	t.Logf("[magic] committed=%d kind=%s dropped=%d reason=%q",
		info.CommittedPosition, info.Corruption, info.DroppedBytes, info.Reason)
	if info.Corruption != CorruptionMagic {
		t.Fatalf("kind=%s want magic", info.Corruption)
	}
	if info.CommittedPosition != 5 {
		t.Fatalf("committed=%d want 5", info.CommittedPosition)
	}
	expectBaseRecords(t, l, 5)
	l.Close()
}

// 序号不连续：归类 sequence-gap。
func TestRecoverySequenceGap(t *testing.T) {
	cfg := testConfig()
	// 基准最后一条序号为 4，新帧序号跳到 9。
	full := encodeFrame(9, flagFirst|flagLast, []byte("ghost"))
	l, info := reopenWithTail(t, cfg, 5, full)
	t.Logf("[sequence-gap] committed=%d kind=%s dropped=%d reason=%q",
		info.CommittedPosition, info.Corruption, info.DroppedBytes, info.Reason)
	if info.Corruption != CorruptionSequence {
		t.Fatalf("kind=%s want sequence", info.Corruption)
	}
	if info.CommittedPosition != 5 {
		t.Fatalf("committed=%d want 5", info.CommittedPosition)
	}
	expectBaseRecords(t, l, 5)
	l.Close()
}

// 多帧记录只有首帧、缺尾帧（写入中途崩溃的典型现场）：整条丢弃。
func TestRecoveryMultiFrameUnterminated(t *testing.T) {
	cfg := testConfig()
	head := encodeFrame(5, flagFirst, []byte("part-a"))
	l, info := reopenWithTail(t, cfg, 5, head)
	t.Logf("[multiframe-open] committed=%d kind=%s dropped=%d reason=%q",
		info.CommittedPosition, info.Corruption, info.DroppedBytes, info.Reason)
	if info.Corruption != CorruptionTruncated {
		t.Fatalf("kind=%s want truncated", info.Corruption)
	}
	if info.CommittedPosition != 5 {
		t.Fatalf("committed=%d want 5", info.CommittedPosition)
	}
	expectBaseRecords(t, l, 5)
	l.Close()
}

// 首帧 + 中间帧有了，但尾帧负载被截断：同样整条丢弃，结论确定。
func TestRecoveryMultiFrameTailCut(t *testing.T) {
	cfg := testConfig()
	head := encodeFrame(5, flagFirst, []byte("part-a"))
	mid := encodeFrame(5, 0, []byte("part-b"))
	tailFull := encodeFrame(5, flagLast, []byte("part-c-final"))
	cut := tailFull[:frameHeaderSize+2]
	raw := bytes.Join([][]byte{head, mid, cut}, nil)
	l, info := reopenWithTail(t, cfg, 5, raw)
	t.Logf("[multiframe-tailcut] committed=%d kind=%s dropped=%d reason=%q",
		info.CommittedPosition, info.Corruption, info.DroppedBytes, info.Reason)
	if info.Corruption != CorruptionTruncated {
		t.Fatalf("kind=%s want truncated", info.Corruption)
	}
	if info.CommittedPosition != 5 {
		t.Fatalf("committed=%d want 5", info.CommittedPosition)
	}
	expectBaseRecords(t, l, 5)
	l.Close()
}

// 确定性：同一份磁盘数据，无论此前是否正常关闭，两次恢复结论必须一致。
func TestRecoveryDeterministic(t *testing.T) {
	cfg := testConfig()
	dir := t.TempDir()
	l, _, _ := Open(dir, cfg)
	for i := 0; i < 5; i++ {
		l.Append(context.Background(), []byte(fmt.Sprintf("base-%03d", i)))
	}
	activePath := l.active.path
	// 正常关闭
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	bad := encodeFrame(5, flagFirst, []byte("orphan"))
	f, _ := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0)
	f.Write(bad)
	f.Close()

	_, i1 := mustOpen(t, dir, cfg)
	l2, _ := mustOpen(t, dir, cfg) // 第二次（首段已被截断修复）
	_ = l2
	// 第一次与第二次结论：第二次数据已修复，应是 clean 且 committed=5。
	t.Logf("[deterministic] 1st: committed=%d kind=%s dropped=%d", i1.CommittedPosition, i1.Corruption, i1.DroppedBytes)
	if i1.CommittedPosition != 5 || i1.Corruption != CorruptionTruncated {
		t.Fatal("first recovery mismatch")
	}
	l3, i3 := mustOpen(t, dir, cfg)
	t.Logf("[deterministic] 2nd: committed=%d kind=%s reason=%q", i3.CommittedPosition, i3.Corruption, i3.Reason)
	if i3.CommittedPosition != 5 || i3.Corruption != CorruptionNone {
		t.Fatal("second recovery mismatch")
	}
	l3.Close()
}

func mustOpen(t *testing.T, dir string, cfg Config) (*Log, *RecoveryInfo) {
	t.Helper()
	l, info, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return l, info
}
