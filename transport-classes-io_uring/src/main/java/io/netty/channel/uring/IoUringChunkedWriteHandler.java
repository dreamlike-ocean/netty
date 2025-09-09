/*
 * Copyright 2025 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.uring;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.IoEvent;
import io.netty.channel.IoEventLoop;
import io.netty.channel.IoRegistration;
import io.netty.channel.unix.Errors;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;

public class IoUringChunkedWriteHandler extends ChunkedWriteHandler {

    private Future<IoRegistration> ioRegistrationFuture;

    private volatile ChunkedIoUringFile currentFile;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        EventLoop eventLoop = ctx.channel().eventLoop();
        if (!(eventLoop instanceof IoEventLoop)) {
            throw new IllegalArgumentException("IoUringChunkedWriteHandler can only be used with an IoEventLoop");
        }

        IoEventLoop ioEventLoop = (IoEventLoop) eventLoop;
        if (!ioEventLoop.isCompatible(IoUringAsyncFileIoHandle.class)) {
            throw new IllegalArgumentException("IoUringChunkedWriteHandler can only be used with an IoEventLoop " +
                    "that is compatible with IoUringIoHandle");
        }
        super.handlerAdded(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ChunkedIoUringFile) {
            ((ChunkedIoUringFile) msg).attach(this);
        }
        super.write(ctx, msg, promise);
    }

    void requestAsyncRead(ChunkedIoUringFile file, ByteBuf readBuffer) {
        this.currentFile = file;
        Future<IoRegistration> registrationFuture = ioRegistrationFuture;
        if (registrationFuture == null) {
            ioRegistrationFuture = ((IoEventLoop) ctx.channel().eventLoop()).register(new IoUringAsyncFileIoHandle())
                    .addListener(future -> {
                        if (ctx.isRemoved()) {
                            return;
                        }
                        if (future.isSuccess()) {
                            IoRegistration ioRegistration = (IoRegistration) future.getNow();
                            ioRegistration.submit(IoUringIoOps.newRead(
                                    file.fd, readBuffer.memoryAddress() + readBuffer.writerIndex(),
                                    file.currentOffset(), readBuffer.writableBytes())
                            );
                            return;
                        }
                        currentFile.handleFailed(new IllegalArgumentException(future.cause()));
                    });
            return;
        }

        if (!ioRegistrationFuture.isSuccess()) {
            currentFile.handleFailed(new IllegalArgumentException(registrationFuture.cause()));
            return;
        }

        // We do not call this function concurrently and this function only is called from the ctx.executor
        // the next call will only occur after the previous one has finished, so this is safe
        ioRegistrationFuture.getNow().submit(IoUringIoOps.newRead(
                file.fd, readBuffer.memoryAddress() + readBuffer.writerIndex(),
                file.currentOffset(), readBuffer.writableBytes())
        );
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        cancelIoRegistration();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        cancelIoRegistration();
    }

    private void cancelIoRegistration() {
        if (!ioRegistrationFuture.isDone()) {
            return;
        }

        if (ioRegistrationFuture.isSuccess()) {
            ioRegistrationFuture.getNow().cancel();
            return;
        }
    }

    class IoUringAsyncFileIoHandle implements IoUringIoHandle {

        @Override
        public void handle(IoRegistration registration, IoEvent ioEvent) {
            // this function is called from the event loop thread
            ChunkedIoUringFile currentFile = IoUringChunkedWriteHandler.this.currentFile;
            IoUringChunkedWriteHandler.this.currentFile = null;
            IoUringIoEvent uringIoEvent = (IoUringIoEvent) ioEvent;

            int res = uringIoEvent.res();
            EventExecutor executor = ctx.executor();
            if (res < 0) {
                if (executor.inEventLoop()) {
                    currentFile.handleFailed(Errors.newIOException("io_uring read", res));
                } else {
                    executor.execute(() -> currentFile.handleFailed(Errors.newIOException("io_uring read", res)));
                }
                return;
            }
            if (executor.inEventLoop()) {
                currentFile.handleSuccess(res);
            } else {
                executor.execute(() -> currentFile.handleSuccess(res));
            }
        }

        @Override
        public void close() throws Exception {
        }
    }
}
