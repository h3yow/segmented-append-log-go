package seglog

import "errors"

var (
	// ErrClosed 日志已关闭。
	ErrClosed = errors.New("seglog: log is closed")
	// ErrRecordTooLarge 单条记录超过配置上限。
	ErrRecordTooLarge = errors.New("seglog: record exceeds max record bytes")
	// ErrBackpressure 在途未刷盘字节达到配置上限，写入被拒绝（背压）。
	ErrBackpressure = errors.New("seglog: inflight bytes limit reached")
	// ErrOffsetReclaimed 读取位置之前的旧数据已被回收。
	ErrOffsetReclaimed = errors.New("seglog: offset has been reclaimed")
	// ErrOffsetOutOfRange 读取位置超出当前可见范围。
	ErrOffsetOutOfRange = errors.New("seglog: offset out of range")
	// ErrTooManyReaders 并发读句柄达到配置上限。
	ErrTooManyReaders = errors.New("seglog: too many open readers")
	// ErrNoCapacity 容量上限（总字节/段数/句柄）耗尽：已按消费进度回收后仍腾不出空间。
	// 与 ErrBackpressure 可区分：后者是瞬时在途字节超限，Sync 后即可恢复；
	// 本错误表示即使回收了所有确定无人再要的数据也写不进去。
	ErrNoCapacity = errors.New("seglog: capacity exhausted")
	// ErrUnknownConsumer 消费者句柄不存在（未注册或已注销）。
	ErrUnknownConsumer = errors.New("seglog: unknown consumer")
)

// CorruptKind 区分恢复时发现的损坏类别。
type CorruptKind int

const (
	CorruptNone CorruptKind = iota
	// CorruptTornTail 尾部残余不完整数据（写入中途崩溃），可安全截断。
	CorruptTornTail
	// CorruptBadMagic 魔数不匹配。
	CorruptBadMagic
	// CorruptBadLength 长度字段非法。
	CorruptBadLength
	// CorruptChecksum CRC 校验失败。
	CorruptChecksum
	// CorruptPartialRecord 跨段记录只写入了一部分（崩溃发生在多段写入中途），
	// 残余片段不会被当作完整记录，恢复时从该记录的首片段处截断。
	CorruptPartialRecord
)

func (k CorruptKind) String() string {
	switch k {
	case CorruptNone:
		return "none"
	case CorruptTornTail:
		return "torn-tail"
	case CorruptBadMagic:
		return "bad-magic"
	case CorruptBadLength:
		return "bad-length"
	case CorruptChecksum:
		return "checksum-mismatch"
	case CorruptPartialRecord:
		return "partial-record"
	}
	return "unknown"
}

// CorruptError 描述恢复/读取时遇到的可区分损坏原因。
type CorruptError struct {
	Kind    CorruptKind
	Segment string
	Offset  uint64
	Detail  string
}

func (e *CorruptError) Error() string {
	return "seglog: corrupt record (" + e.Kind.String() + ") in " + e.Segment + " @" + itoa(e.Offset) + ": " + e.Detail
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
