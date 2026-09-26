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
