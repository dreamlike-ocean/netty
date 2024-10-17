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
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import java.nio.channels.FileChannel;

public class IoUringAsyncFile {

    private final FileDescriptor fileDescriptor;
    private final int nativeFd;

    private IoEventLoop ioEventLoop;

    private IoRegistration ioRegistration;

    private volatile boolean registered;

    private short opsId = Short.MIN_VALUE;

    private final LongObjectHashMap<Promise<AsyncFileOperatorResult>> processingTask;

    public IoUringAsyncFile(FileChannel fileChannel) {
        this.fileDescriptor = new FileDescriptor(Native.getFd(fileChannel));
        this.nativeFd = Native.getFd(fileChannel);
        this.processingTask = new LongObjectHashMap<>();
    }

    public Future<IoUringAsyncFile> register(EventLoop eventLoop) {
        DefaultPromise<IoUringAsyncFile> promise = new DefaultPromise<>(eventLoop);
        if (ioRegistration != null) {
            promise.setFailure(new IllegalStateException("registered to an event loop already"));
            return promise;
        }

        boolean isIoEventLoop = eventLoop instanceof IoEventLoop;
        if (!isIoEventLoop || ((IoEventLoop) eventLoop).isCompatible(InternalUnsafeHandle.class)) {
            promise.setFailure(new IllegalStateException("incompatible event loop type: " + eventLoop.getClass().getName()));
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

    public Future<AsyncFileOperatorResult> read(ByteBuf buf, int len, int offset) {
        DefaultPromise<AsyncFileOperatorResult> promise = new DefaultPromise<>(ioEventLoop);

        if(!registered) {
            promise.setFailure(new IllegalStateException("register first"));
        }

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

        promise.addListener(new GenericFutureListener<Future<? super AsyncFileOperatorResult>>() {
            @Override
            public void operationComplete(Future<? super AsyncFileOperatorResult> future) throws Exception {
                // release buf
                buf.release();
            }
        });
        return promise;
    }



    private void read0(ByteBuf buf, int len, int offset, DefaultPromise<AsyncFileOperatorResult> promise) {
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

    public Future<AsyncFileOperatorResult> write(ByteBuf buf, int len, int offset) {
        DefaultPromise<AsyncFileOperatorResult> promise = new DefaultPromise<>(ioEventLoop);

        if(!registered) {
            promise.setFailure(new IllegalStateException("register first"));
        }


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
        promise.addListener(new GenericFutureListener<Future<? super AsyncFileOperatorResult>>() {
            @Override
            public void operationComplete(Future<? super AsyncFileOperatorResult> future) throws Exception {
                // release buf
                buf.release();
            }
        });

        return promise;
    }

    private void write0(ByteBuf buf, int len, int offset, DefaultPromise<AsyncFileOperatorResult> promise) {
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
