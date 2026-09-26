# segmented-append-log-go

可崩溃恢复的分段式追加日志存储组件（包名 `seglog`），作为上层数据库或消息系统的本地持久化底座。仅依赖 Go 标准库。

## 存储格式与版本兼容

每条记录的二进制格式（格式版本 **v1**）：

```
[magic uint32 LE][length uint32 LE][crc32 uint32 LE][payload ...]
```

- `magic` 固定为 `0x53514C31`（"SQL1"），标识 v1 格式，用于定位记录边界与版本识别。
- `length` 为 payload 字节数。
- `crc32`（IEEE）覆盖 `length` 字段与 `payload`，用于检测半写与位翻转。

数据目录下按段存储，段文件名为 `%020d.seg`，数字是该段第一条记录的逻辑偏移（base offset）。段写满 `MaxSegmentBytes` 后滚动新段。

**兼容规则**：魔数不匹配的头部一律视为非法数据（`bad-magic`），不会被当作任何旧版本格式解析。未来版本演进应通过更换 magic 实现，读取方只接受已知的 magic。

## 恢复语义

`Open` 时逐段顺序解码记录，推导确定的可用范围：

- 扫描在遇到第一条无法完整解码的记录时停止，该位置即**有效数据终点**；其后所有字节（含后续段）被截断/删除，恢复结论与正常关闭后再打开完全一致。
- 损坏原因可区分（`RecoveryInfo.Corrupt` 与 `*CorruptError.Kind`）：
  - `torn-tail`：尾部残余不完整数据（写入中途崩溃），安全截断；
  - `bad-magic`：魔数不匹配；
  - `bad-length`：长度字段非法；
  - `checksum-mismatch`：CRC 校验失败。
- 恢复后 `NextOffset == ValidEnd`，追加从该位置继续，偏移连续不跳号。
- 残余数据永远不会作为完整记录返回。

## 并发与背压

- `Append` 在互斥锁下分配逻辑偏移，多写并发时偏移全局唯一、连续不跳号；读方只能看到完整且已提交的记录。
- 读并发通过槽位（`MaxReaders`）限流，超限立即返回 `ErrTooManyReaders` 而非阻塞。
- 未刷盘字节达到 `MaxInflightBytes` 时，`Append` 返回 `ErrBackpressure`；调用 `Sync` 后恢复可写。不会静默丢弃，也不会阻塞整个进程。
- 单条记录超过 `MaxRecordBytes` 返回 `ErrRecordTooLarge`。

## 回收

`Reclaim(before)` 整体删除完全落在逻辑偏移 `before` 之前的非活跃段。仍被读取位置引用的段不会被回收；读取已回收位置返回 `ErrOffsetReclaimed`，与 `ErrOffsetOutOfRange` 可区分。

## 可配置项（`Config`）

| 字段 | 含义 | 默认值 |
|---|---|---|
| `Dir` | 数据目录 | 必填 |
| `MaxSegmentBytes` | 单段最大字节数，超过滚动新段 | 64 MiB |
| `MaxRecordBytes` | 单条记录最大字节数 | 4 MiB |
| `MaxInflightBytes` | 在途未刷盘字节上限（背压阈值） | 16 MiB |
| `MaxReaders` | 并发读槽位上限 | 128 |
| `SyncOnAppend` | 每次追加后立即刷盘 | false |

## 本地验证

```bash
go build ./...
go vet ./...
go test ./seglog/ -v        # 功能与崩溃恢复测试，日志打印恢复位置与错误归类
go test ./seglog/ -race     # 并发竞态检测
```

测试覆盖：写入各阶段异常终止（半个头 / 半个负载 / 坏魔数 / 坏长度 / 坏 CRC）、正常关闭与异常终止恢复结论一致性、段滚动与回收、多写多读并发、背压与上限拒绝。
