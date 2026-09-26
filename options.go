package seglog

// options.go：可配置项与默认值。

const (
	defaultMaxSegmentBytes = 64 << 20 // 64 MiB
	defaultMaxOpenFiles    = 16
)

// Config 控制分段、刷盘、限流与资源上限行为。零值 Config 配合
// DefaultConfig 使用；未设置的项在 Open 时由 withDefaults 填充。
type Config struct {
	// MaxSegmentBytes 单个分段文件的目标大小；超过后滚动新分段。
	// 单条记录可以大于该值（独占一个分段），除非超过 MaxRecordBytes。
	MaxSegmentBytes int64

	// MaxRecordBytes 单条记录允许的最大字节数；0 表示不限制。
	MaxRecordBytes int

	// MaxSegments 保留分段数量上限（含活动分段）；达到上限且无可回收
	// 空间时，写入返回 ErrStorageLimit。0 表示不限制。
	MaxSegments int

	// MaxTotalBytes 全部分段总字节数上限；0 表示不限制。
	MaxTotalBytes int64

	// MaxOpenFiles 允许同时打开的分段文件句柄数（含活动分段，>=1）。
	MaxOpenFiles int

	// FsyncEveryWrite 为 true 时每次 Append 都 fsync。为 false 时只在
	// 滚动分段与 Close 时 fsync（进程崩溃可能丢失尾部未落盘记录，
	// 但恢复结论依旧确定：丢失的记录等同于写入中断的残余）。
	FsyncEveryWrite bool

	// RetainBytes 回收保留水位：起始偏移早于
	// (totalBytes - RetainBytes) 的分段才可被回收；0 表示不额外保留
	// （只要所有读取方都已越过即可回收）。
	RetainBytes int64

	// MaxChunkBytes 单帧负载上限，超过则把一条记录拆成多帧；
	// <=0 时取 MaxSegmentBytes/2 或默认值。仅用于控制帧大小。
	MaxChunkBytes int

	// 测试与故障注入使用的钩子，生产路径保持 nil。
	// 钩子返回错误时 Append 中止并返回该错误（用于模拟写入中途失败）。
	beforeFrameSync func(phase syncPhase) error
}

// syncPhase 标识一次写盘流程所处阶段，供崩溃注入使用。
type syncPhase int

const (
	phaseBeforeWrite syncPhase = iota
	phaseAfterChunks
	phaseBeforeFsync
	phaseAfterFsync
)

// DefaultConfig 返回带保守默认值的配置。
func DefaultConfig() Config {
	return Config{
		MaxSegmentBytes: defaultMaxSegmentBytes,
		MaxOpenFiles:    defaultMaxOpenFiles,
	}
}

// withDefaults 填充零值默认项并做基本修正。
func (c Config) withDefaults() Config {
	out := c
	if out.MaxSegmentBytes <= 0 {
		out.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if out.MaxOpenFiles < 2 {
		// 至少给活动分段与一个只读分段留出句柄。
		out.MaxOpenFiles = defaultMaxOpenFiles
	}
	if out.MaxChunkBytes <= 0 {
		// 默认每帧负载约为分段大小的一半，保证一条超大记录至少被拆分。
		out.MaxChunkBytes = int(out.MaxSegmentBytes / 2)
		if out.MaxChunkBytes < 4096 {
			out.MaxChunkBytes = 4096
		}
	}
	if out.MaxRecordBytes < 0 {
		out.MaxRecordBytes = 0
	}
	return out
}

// chunkSize 返回写入时单帧负载的字节上限（>=1）。
func (c Config) chunkSize() int {
	n := c.MaxChunkBytes
	if n <= 0 {
		n = int(c.MaxSegmentBytes / 2)
		if n < 4096 {
			n = 4096
		}
	}
	return n
}
