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

## 跨段记录

当一条记录编码后超过 `MaxSegmentBytes`、任何单个空段都放不下时，它会被拆成连续多个**分片**（格式版本 **v2**）写入后续分段，读取时仍作为一条完整记录返回。分片格式：

```
[magic uint32 LE][length uint32 LE][crc32 uint32 LE][flags uint8][chunk ...]
```

- `magic` 为 `0x53514C32`（"SQL2"）；`length = 1 + len(chunk)`；`crc32` 覆盖 `length`、`flags` 与 `chunk`。
- `flags`：`FIRST`（首分片）/ `LAST`（末分片）。一条逻辑记录 = FIRST + 0..n 个中间分片 + LAST，分片在段序列中连续。
- 首分片所在段文件名与普通段相同（`%020d.seg`）；续段文件名带分片序号 `%020d-%04d.seg`，base 都是被拆记录的逻辑偏移。
- 记录只有全部落盘后才提交逻辑偏移（偏移连续不跳号）；段内记录计数把整条跨段记录计在首分片所在段。

**恢复规则**：扫描时若跨段记录只写了一半（进程在多段写入中途崩溃），残余分片不会被当作完整记录，归类为 `partial-record`（`CorruptPartialRecord`），与 `torn-tail`（单个记录/分片的字节没写完）、`bad-magic`、`bad-length`、`checksum-mismatch` 都可区分；恢复从该记录的首分片处整体截断（可能跨回之前的段），后续段文件被删除。续段开头残留的分片（首分片所在段已被回收）在恢复时被跳过，不算损坏。

## 恢复语义

`Open` 时逐段顺序解码记录与分片，推导确定的可用范围：

- 扫描在遇到第一条无法完整解码的记录/分片时停止，该位置即**有效数据终点**；其后所有字节（含后续段）被截断/删除，恢复结论与正常关闭后再打开完全一致。
- 损坏原因可区分（`RecoveryInfo.Corrupt` 与 `*CorruptError.Kind`）：`torn-tail`、`bad-magic`、`bad-length`、`checksum-mismatch`、`partial-record`（跨段记录写了一半）。
- 恢复后 `NextOffset == ValidEnd`，追加从该位置继续，偏移连续不跳号。
- 残余数据永远不会作为完整记录返回。

## 并发与背压

- `Append` 在互斥锁下分配逻辑偏移，多写并发时偏移全局唯一、连续不跳号；读方只能看到完整且已提交的记录。
- 读并发通过槽位（`MaxReaders`）限流，超限立即返回 `ErrTooManyReaders` 而非阻塞。
- 未刷盘字节达到 `MaxInflightBytes` 时，`Append` 返回 `ErrBackpressure`；调用 `Sync` 后恢复可写。不会静默丢弃，也不会阻塞整个进程。
- 单条记录超过 `MaxRecordBytes` 返回 `ErrRecordTooLarge`。

## 容量上限

三类资源天花板，超限时的处理顺序：**先按消费进度自动回收**确定无人再要的旧段；仍不够时 `Append` 立即返回 **`ErrNoCapacity`**——与 `ErrBackpressure`（瞬时，Sync 后恢复）可区分，不静默丢数据、不阻塞进程、句柄不超开。

- `MaxTotalBytes`：数据目录总字节数。
- `MaxSegments`：段文件数量（含跨段记录的续段）。
- `MaxOpenFiles`：同时打开的底层文件句柄数。段句柄懒打开 + LRU 淘汰空闲句柄，写路径拿不到句柄时返回 `ErrNoCapacity`，读路径返回 `ErrTooManyReaders`。

## 回收与消费进度

读取方通过消费者 API 显式上报消费进度：

- `RegisterConsumer(at)` 注册消费者，返回句柄；`Commit(id, off)` 上报"已消费到逻辑偏移 `off`（不含）"，进度只前进不回退；`UnregisterConsumer(id)` 注销。
- **回收地平线** = 所有注册消费者已消费位置的最小值（`ReclaimableOffset()` 可查询）。回收只清掉地平线之前、且不再被任何读取位置引用的整段；慢消费者没跟上的数据不会被回收。
- 没有注册消费者时，地平线视为最新写入位置（全部可回收）。
- `Reclaim(before)` 的实际回收范围会被钳制到地平线以内；`Append` 触发容量上限时也会先按地平线自动回收。
- 读取已回收位置返回 `ErrOffsetReclaimed`，与从未写过的位置（`ErrOffsetOutOfRange`）可区分。
- 跨段记录的首分片段被回收而续段保留时，该记录判定为已回收（`ErrOffsetReclaimed`），续段中起始的后续记录不受影响。

## 可配置项（`Config`）

| 字段 | 含义 | 默认值 |
|---|---|---|
| `Dir` | 数据目录 | 必填 |
| `MaxSegmentBytes` | 单段最大字节数，超过滚动新段 | 64 MiB |
| `MaxRecordBytes` | 单条记录最大字节数 | 4 MiB |
| `MaxInflightBytes` | 在途未刷盘字节上限（背压阈值） | 16 MiB |
| `MaxReaders` | 并发读槽位上限 | 128 |
| `SyncOnAppend` | 每次追加后立即刷盘 | false |
| `MaxTotalBytes` | 数据目录总字节上限，0 = 不限 | 0（不限） |
| `MaxSegments` | 段文件数量上限（含续段），0 = 不限 | 0（不限） |
| `MaxOpenFiles` | 同时打开的文件句柄上限，0 = 不限 | 0（不限） |

## 本地验证

```bash
go build ./...
go vet ./...
go test ./seglog/ -v        # 功能与崩溃恢复测试，日志打印恢复位置与错误归类
go test ./seglog/ -race     # 并发竞态检测
```

测试覆盖：写入各阶段异常终止（半个头 / 半个负载 / 坏魔数 / 坏长度 / 坏 CRC）、跨段记录在各写入阶段中断（只有首分片 / 续段分片头写一半 / 续段分片负载写一半 / 中间分片完整但缺末分片 / 全部写完未关闭）、正常关闭与异常终止恢复结论一致性、段滚动与回收、容量三项上限（总字节 / 段数 / 句柄）与 `ErrNoCapacity` 归类、按消费进度的回收门控（慢消费者钉住数据）、跨段记录首分片段回收后续段可读、多写多读并发与回收/刷盘并发。
