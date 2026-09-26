package seglog

import "errors"

// 可区分的错误原因。调用方可用 errors.Is 判定具体类别。
var (
	// ErrClosed 表示日志或读取器已关闭。
	ErrClosed = errors.New("seglog: closed")
	// ErrIncompleteRecord 表示尾部存在残余数据（帧头或帧体不完整、
	// 或分片记录缺少结尾），不能作为完整记录返回。
	ErrIncompleteRecord = errors.New("seglog: incomplete record at tail")
	// ErrChecksumMismatch 表示帧结构完整但 CRC 校验失败。
	ErrChecksumMismatch = errors.New("seglog: checksum mismatch")
	// ErrInvalidMagic 表示帧头魔数无法识别（格式损坏或非本组件文件）。
	ErrInvalidMagic = errors.New("seglog: invalid frame magic")
	// ErrSequenceGap 表示记录序号不连续，中间存在缺失。
	ErrSequenceGap = errors.New("seglog: sequence gap")
	// ErrRecordTooLarge 表示单条记录超过配置的 MaxRecordBytes。
	ErrRecordTooLarge = errors.New("seglog: record too large")
	// ErrStorageLimit 表示分段数或总字节数达到配置上限，
	// 且没有可回收空间，写入被明确拒绝（背压）。
	ErrStorageLimit = errors.New("seglog: storage limit reached")
	// ErrNoRecord 表示当前没有已提交的新记录可供非阻塞读取。
	ErrNoRecord = errors.New("seglog: no committed record")
	// ErrPositionReclaimed 表示要读取的位置所在分段已被回收。
	ErrPositionReclaimed = errors.New("seglog: position reclaimed")
	// ErrBadPosition 表示给定位置非法（落在记录边界之外等）。
	ErrBadPosition = errors.New("seglog: bad position")
)

// CorruptionKind 是恢复阶段对尾部/数据损坏的归类。
type CorruptionKind string

const (
	CorruptionNone      CorruptionKind = "none"
	CorruptionTruncated CorruptionKind = "truncated-tail"
	CorruptionChecksum  CorruptionKind = "checksum-mismatch"
	CorruptionMagic     CorruptionKind = "invalid-magic"
	CorruptionSequence  CorruptionKind = "sequence-gap"
)

// SegmentMeta 描述恢复后单个分段的元信息。
type SegmentMeta struct {
	BasePosition uint64
	Size         int64
}

// RecoveryInfo 描述一次打开/恢复得到的确定性结论。
type RecoveryInfo struct {
	// FirstPosition 当前仍可读取的最早逻辑位置。
	FirstPosition uint64
	// CommittedPosition 已提交位置（下一条待写入记录的序号）。
	CommittedPosition uint64
	// TailPath 被截断修复的尾部分段文件名；为空表示无需修复。
	TailPath string
	// KeptBytes 尾部分段保留的有效字节数。
	KeptBytes int64
	// DroppedBytes 尾部被丢弃的残余字节数。
	DroppedBytes int64
	// Corruption 损坏归类。
	Corruption CorruptionKind
	// Reason 人类可读的恢复说明。
	Reason string
	// Segments 恢复后保留的全部分段（按位置升序）。
	Segments []SegmentMeta
}
