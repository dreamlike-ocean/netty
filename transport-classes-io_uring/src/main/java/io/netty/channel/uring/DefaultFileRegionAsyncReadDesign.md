# `DefaultFileRegion` 异步读取设计 / DefaultFileRegion Async Read Design

## 中文

### 目标

当 `splice` 不使用时，为 `DefaultFileRegion` 提供一条更贴近 io_uring 模型的 fallback 路径。

此前 `FileRegion` 的 fallback 路径是：

1. 同步把文件数据拷贝到 direct `ByteBuf`
2. 再提交 `SEND`

这个设计为 `DefaultFileRegion` 增加了一条专用路径：

1. 先对文件描述符提交 `IORING_OP_READ`
2. 再对读出的 chunk 提交 `SEND` 或 `SEND_ZC`

默认策略保持不变：

- 如果支持 `splice`，`DefaultFileRegion` 仍然走 `IoUringFileRegion`
- 否则 `DefaultFileRegion` 走新的 async read fallback
- 泛型 `FileRegion` 实现仍然沿用现有的 chunked `transferTo(...)` fallback

为了测试和实验，保留了一个内部系统属性开关：

- `io.netty.iouring.forceAsyncDefaultFileRegion=true`

### 路由选择

`AbstractIoUringStreamChannel.filterOutboundMessage(...)` 现在按下面的方式路由出站消息：

- 当 `splice` 可用且没有强制 async fallback 时，`DefaultFileRegion -> IoUringFileRegion`
- 其他情况下，`DefaultFileRegion -> IoUringReadFileRegion`
- 其他 `FileRegion` 实现保持原样透传

`IoUringReadFileRegion` 是一个包裹 `DefaultFileRegion` 的轻量包装类。

它维护两个计数器：

- `readTransferred`: 已经从文件读入当前异步管线的字节数
- `transferred`: 已经在 socket 写侧真正完成的字节数

这个拆分很重要。`FileRegion.transferred()` 仍然必须表示写进度，而不是“已经从磁盘预读出来的字节数”。

### Completion 路由

`IORING_OP_READ` 原本已经用于入站 socket read，所以新的出站文件读取 completion 不能直接复用现有 read 路径。

这个设计把负值 `data` 保留为内部标记，同时让常规 op id 始终保持为正数：

- `AbstractIoUringChannel.nextOpsId()` 现在只生成正数 id
- `FILE_REGION_READ_DATA = Short.MIN_VALUE` 用来标记出站文件读取 completion

`AbstractIoUringChannel.handle(...)` 通过 `isWriteIoOp(op, data)` 判断这类 completion，从而把 `IoUringReadFileRegion` 的 `IORING_OP_READ` 分发到 `writeComplete(...)`，而不是 `readComplete(...)`。

### 写入流水线

对于 `IoUringReadFileRegion`，写侧状态机如下：

1. 分配一个不超过 `FILE_REGION_MAX_CHUNK_SIZE` 的 direct chunk buffer
2. 提交 `IORING_OP_READ(fileFd, offset, chunkBuffer)`
3. 在 read completion 时：
   - 推进 `readTransferred`
   - 设置 chunk buffer 的 `writerIndex`
   - 提交 `SEND` 或 `SEND_ZC`
4. 在 send completion 时：
   - 推进 `transferred`
   - 推进 outbound progress
   - 如果当前 chunk 只是部分写出，则继续重提剩余字节
   - 如果当前 chunk 已经全部写完且整个 region 也结束了，则从 outbound buffer 移除
   - 否则启动下一轮文件 `READ`

由于 `READ` completion 会在回调里继续内联提交下一个 write op，`AbstractIoUringChannel` 需要暴露 `incrementOutstandingWrites(...)`，让写调度器能正确统计额外新增的 pending CQE。

### Zero-Copy 策略

`IoUringSocketChannel` 负责决定 socket 发送阶段是否使用 `SEND_ZC`。

策略如下：

- 只有 async-read 路径的 send 阶段可以选择 `SEND_ZC`
- 是否启用仍然遵循 `IO_URING_WRITE_ZERO_COPY_THRESHOLD`
- 文件读取本身并不是 zero-copy，这里只是 socket 发送阶段的优化

如果 `SEND_ZC` 产生了延迟通知 CQE：

- chunk buffer 会一直 retain 到通知到达为止
- 常规 write completion 仍然会立刻推进 outbound progress
- 延迟通知只负责最终的 buffer release

这条路径还必须容忍 channel 关闭后才到达的延迟通知，因此 `IoUringSocketUnsafe.writeComplete0(...)` 不能假设 `outboundBuffer()` 一定非空。

### 错误处理

关键错误处理规则如下：

- `READ res == 0` 时会校验底层文件大小，并把意外的短文件内容当成错误处理
- 被取消的 read/write op 会释放当前 chunk buffer
- 可重试的 socket write 失败会保留 chunk buffer，等待在 `POLLOUT` 时重提剩余数据
- 不可重试的失败会释放 chunk buffer，并让写路径失败

### 测试

`transport-native` 测试覆盖了以下场景：

- 强制让 `DefaultFileRegion` 走 async-read 路由
- async 路径上的多 `region` 连续写入
- 带 `SEND_ZC` 的 async 路径
- 现有 `SEND_ZC` 生命周期测试，确保新的通知处理不会让 buffer ownership 回归

这个设计保持 `splice` 仍然是默认快路径，同时让非 `splice` fallback 更像一条真正的 io_uring pipeline，而不是“先同步拷文件，再异步发 socket”的折中实现。

## English

### Goal

Provide an io_uring-native fallback for `DefaultFileRegion` when `splice` is not used.

The previous fallback path for `FileRegion` was:

1. synchronously copy file data into a direct `ByteBuf`
2. submit `SEND`

This design adds a dedicated path for `DefaultFileRegion`:

1. submit `IORING_OP_READ` against the file descriptor
2. submit `SEND` or `SEND_ZC` for the loaded chunk

The default strategy remains unchanged:

- if `splice` is supported, `DefaultFileRegion` still uses `IoUringFileRegion`
- otherwise `DefaultFileRegion` uses the async read fallback
- generic `FileRegion` implementations still use the existing chunked `transferTo(...)` fallback

An internal system property exists for testing and experiments:

- `io.netty.iouring.forceAsyncDefaultFileRegion=true`

### Routing

`AbstractIoUringStreamChannel.filterOutboundMessage(...)` routes outbound messages as follows:

- `DefaultFileRegion -> IoUringFileRegion` when `splice` is available and async fallback is not forced
- `DefaultFileRegion -> IoUringReadFileRegion` otherwise
- other `FileRegion` implementations pass through unchanged

`IoUringReadFileRegion` is a lightweight wrapper around `DefaultFileRegion`.

It keeps two counters:

- `readTransferred`: bytes already read from the file into the current async pipeline
- `transferred`: bytes actually completed on the socket write side

That split is important. `FileRegion.transferred()` must continue to mean write progress, not "bytes prefetched from disk".

### Completion Routing

`IORING_OP_READ` already existed for inbound socket reads, so the new outbound file-read completion cannot reuse the normal read path.

The design reserves negative `data` values for internal markers and keeps normal op ids positive:

- `AbstractIoUringChannel.nextOpsId()` now only produces positive ids
- `FILE_REGION_READ_DATA = Short.MIN_VALUE` marks outbound file-read completions

`AbstractIoUringChannel.handle(...)` checks `isWriteIoOp(op, data)` so `IORING_OP_READ` for `IoUringReadFileRegion` is delivered to `writeComplete(...)` instead of `readComplete(...)`.

### Write Pipeline

For `IoUringReadFileRegion`, the write side uses the following state machine:

1. allocate a direct chunk buffer up to `FILE_REGION_MAX_CHUNK_SIZE`
2. submit `IORING_OP_READ(fileFd, offset, chunkBuffer)`
3. on read completion:
   - advance `readTransferred`
   - set the chunk buffer `writerIndex`
   - submit `SEND` or `SEND_ZC`
4. on send completion:
   - advance `transferred`
   - advance outbound progress
   - if the chunk is only partially written, resubmit the remaining bytes
   - if the chunk is fully written and the region is complete, remove it from the outbound buffer
   - otherwise start the next file `READ`

Because `READ` completion submits another write op inline, `AbstractIoUringChannel` exposes `incrementOutstandingWrites(...)` so the write scheduler can account for the extra pending CQE.

### Zero-Copy Policy

`IoUringSocketChannel` decides whether the socket send side should use `SEND_ZC`.

Policy:

- only the send stage of the async-read path may use `SEND_ZC`
- selection still uses `IO_URING_WRITE_ZERO_COPY_THRESHOLD`
- the file read itself is not zero-copy; this is only a socket-side optimization

If `SEND_ZC` produces a deferred notification CQE:

- the chunk buffer is retained until the notification arrives
- the normal write completion still advances outbound progress immediately
- the delayed notification only controls final buffer release

This path must also tolerate late notifications after the channel has already closed, so `IoUringSocketUnsafe.writeComplete0(...)` cannot assume `outboundBuffer()` is still non-null.

### Error Handling

Important error handling rules:

- `READ res == 0` validates the underlying file size and treats unexpected short file content as an error
- canceled read/write ops release the current chunk buffer
- retryable socket write failures keep the chunk buffer so the remainder can be resubmitted on `POLLOUT`
- non-retryable failures release the chunk buffer and fail the write path

### Tests

The transport-native test suite covers:

- forced async-read routing for `DefaultFileRegion`
- multi-region writes on the async path
- async path with `SEND_ZC`
- existing `SEND_ZC` lifecycle tests to ensure the new notification handling does not regress buffer ownership

This design keeps `splice` as the default fast path while making the non-`splice` fallback behave like a real io_uring pipeline instead of a synchronous file copy followed by an async socket send.
