/*
 * Copyright 2024 The Netty Project
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
import io.netty.channel.*;
import io.netty.channel.unix.Errors;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;

public class IoUringAsyncFile extends AbstractReferenceCounted {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(IoUringAsyncFile.class);

    private final FileDescriptor fileDescriptor;

    private final int nativeFd;

    protected IoEventLoop ioEventLoop;

    private IoRegistration ioRegistration;

    private volatile boolean registered;

    private short opsId = Short.MIN_VALUE;

    private FileChannel javaFileChannel;

    private final LongObjectHashMap<Promise<AsyncFileOperatorResult>> processingTask;

    public IoUringAsyncFile(FileChannel fileChannel) {
        this.fileDescriptor = new FileDescriptor(Native.getFd(fileChannel));
        this.nativeFd = Native.getFd(fileChannel);
        this.javaFileChannel = fileChannel;
        this.processingTask = new LongObjectHashMap<>();
    }

    public Future<IoUringAsyncFile> register(EventLoop eventLoop) {

        if (ioRegistration != null) {
           throw new IllegalStateException("registered to an event loop already");
        }

        DefaultPromise<IoUringAsyncFile> promise = new DefaultPromise<>(eventLoop);
        boolean isIoEventLoop = eventLoop instanceof IoEventLoop;
        if (!isIoEventLoop || !((IoEventLoop) eventLoop).isCompatible(InternalUnsafeHandle.class)) {
            promise.setFailure(
                    new IllegalStateException("incompatible event loop type: " + eventLoop.getClass().getName())
            );
        }

        IoEventLoop ioEventLoop = (IoEventLoop) eventLoop;
        this.ioEventLoop = ioEventLoop;

        IoUringAsyncFile thisFile = this;
        ioEventLoop.register(new InternalUnsafeHandle())
                // Use anonymous classes to avoid the warm-up issue of the initial access of lambdas.
                .addListener(new GenericFutureListener<Future<? super IoRegistration>>() {
                    @Override
                    public void operationComplete(Future<? super IoRegistration> r) throws Exception {
                        if (r.isSuccess()) {
                            ioRegistration = (IoUringIoRegistration) r.getNow();
                            promise.setSuccess(thisFile);
                            registered = true;
                        } else {
                            promise.setFailure(r.cause());
                        }
                    }
                });

        return promise;
    }

    public Future<AsyncFileOperatorResult> read(ByteBuf buf, int len, long offset) {

        if (!registered) {
           throw new IllegalStateException("register first");
        }

        DefaultPromise<AsyncFileOperatorResult> promise = new DefaultPromise<>(ioEventLoop);

        if (buf.writableBytes() < len) {
            promise.setFailure(new IllegalArgumentException("buf writableBytes must greater than len!"));
        }

        //retain for io_uring read
        buf.retain();
        if (ioEventLoop.inEventLoop()) {
            read0(buf, len, offset, promise);
        } else {
            ioEventLoop.execute(new Runnable() {
                @Override
                public void run() {
                    read0(buf, len, offset, promise);
                }
            });
        }

        int writerIndex = buf.writerIndex();

        promise.addListener(new GenericFutureListener<Future<? super AsyncFileOperatorResult>>() {
            @Override
            public void operationComplete(Future<? super AsyncFileOperatorResult> future) throws Exception {
                // release buf
                buf.release();

                if (future.isSuccess()) {
                    buf.writerIndex(writerIndex + ((AsyncFileOperatorResult) future.getNow()).result);
                }
            }
        });
        return promise;
    }

    private void read0(ByteBuf buf, int len, long offset, DefaultPromise<AsyncFileOperatorResult> promise) {
        short nextOpsId = nextOpsId();

        processingTask.put(nextOpsId, promise);
        try {
            long readId = ioRegistration.submit(new IoUringIoOps(
                    Native.IORING_OP_READ,
                    0,
                    (short) 0,
                    nativeFd,
                    0,
                    buf.memoryAddress() + buf.writerIndex(),
                    len,
                    offset,
                    nextOpsId
            ));

        } catch (Exception e) {
            promise.setFailure(e);
            processingTask.remove(nextOpsId);
        }
    }

    public Future<AsyncFileOperatorResult> write(ByteBuf buf, int len, long offset) {

        if (!registered) {
            throw new IllegalStateException("register first");
        }

        DefaultPromise<AsyncFileOperatorResult> promise = new DefaultPromise<>(ioEventLoop);

        if (buf.readableBytes() < len) {
            promise.setFailure(new IllegalArgumentException("buf readableBytes must greater than len!"));
        }

        buf.retain();
        if (ioEventLoop.inEventLoop()) {
            write0(buf, len, offset, promise);
        } else {
            ioEventLoop.execute(new Runnable() {
                @Override
                public void run() {
                    write0(buf, len, offset, promise);
                }
            });
        }

        int readerIndex = buf.readerIndex();

        promise.addListener(new GenericFutureListener<Future<? super AsyncFileOperatorResult>>() {
            @Override
            public void operationComplete(Future<? super AsyncFileOperatorResult> future) throws Exception {
                // release buf
                buf.release();

                if (future.isSuccess()) {
                    buf.readerIndex(readerIndex + ((AsyncFileOperatorResult) future.getNow()).result);
                }
            }
        });

        return promise;
    }

    private void write0(ByteBuf buf, int len, long offset, DefaultPromise<AsyncFileOperatorResult> promise) {
        short nextOpsId = nextOpsId();

        processingTask.put(nextOpsId, promise);
        try {
            long writeId = ioRegistration.submit(new IoUringIoOps(
                    Native.IORING_OP_WRITE,
                    0,
                    (short) 0,
                    nativeFd,
                    0,
                    buf.memoryAddress(),
                    len,
                    offset,
                    nextOpsId
            ));
        } catch (Exception e) {
            promise.setFailure(e);
            processingTask.remove(nextOpsId);
        }
    }

    protected final short nextOpsId() {
        short id = opsId++;

        // We use 0 for "none".
        if (id == 0) {
            id = opsId++;
        }
        return id;
    }

    @Override
    protected void deallocate() {
        FileChannel file = this.javaFileChannel;

        if (file == null) {
            return;
        }
        this.javaFileChannel = null;

        try {
            file.close();
        } catch (IOException e) {
            logger.warn("Failed to close a file.", e);
        }
    }

    @Override
    public IoUringAsyncFile touch(Object hint) {
        return this;
    }

    @Override
    public IoUringAsyncFile retain() {
         super.retain();
         return this;
    }

    @Override
    public IoUringAsyncFile retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public IoUringAsyncFile touch() {
        return this;
    }

    class InternalUnsafeHandle implements IoUringIoHandle {

        @Override
        public void handle(IoRegistration registration, IoEvent ioEvent) {
            IoUringIoEvent ioUringIoEvent = (IoUringIoEvent) ioEvent;
            Promise<AsyncFileOperatorResult> promise = processingTask.get(ioUringIoEvent.data());
            int res = ioUringIoEvent.res();
            if (res < 0) {
                promise.setFailure(new Errors.NativeIoException("IoUringAsyncFileRead", -res));
                return;
            }

            promise.setSuccess(new AsyncFileOperatorResult(IoUringAsyncFile.this, res));
        }

        @Override
        public void close() throws Exception {
            //todo
        }
    }

    public static class AsyncFileOperatorResult {
        private final IoUringAsyncFile file;

        private final int result;

        public AsyncFileOperatorResult(IoUringAsyncFile file, int result) {
            this.file = file;
            this.result = result;
        }

        public IoUringAsyncFile getFile() {
            return file;
        }

        public int getResult() {
            return result;
        }
    }
}
