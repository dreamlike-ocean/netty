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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.IoEvent;
import io.netty.channel.IoEventLoop;
import io.netty.channel.IoRegistration;
import io.netty.channel.unix.Errors;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedNioFile;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link ChunkedInput} that fetches data from a file chunk by chunk using
 * IoUring {@link FileChannel}.
 * <p>
 * If your operating system supports
 * <a href="https://man7.org/linux/man-pages/man3/io_uring_prep_splice.3.html">zero-copy file transfer</a>
 * you might want to use {@link io.netty.channel.DefaultFileRegion} instead.
 */
public class ChunkedIoUringFile extends ChunkedNioFile {

    static final int DEFAULT_CHUNK_SIZE = 8192;

    private final int fd;
    private final IoUringAsyncFileIoHandle ioUringAsyncFileIoHandle;
    private final IoUringChunkedWriteHandler ioUringChunkedWriteHandler;
    private Future<IoRegistration> registerFuture;
    private volatile IoRegistration registration;
    private volatile AtomicReference<ByteBuf> readBufferReference;
    private volatile Exception cause;

    /**
     * Creates a new instance that fetches data from the specified file.
     */
    public ChunkedIoUringFile(File in, IoUringChunkedWriteHandler ioUringChunkedWriteHandler) throws IOException {
        this(new RandomAccessFile(in, "r").getChannel(), ioUringChunkedWriteHandler);
    }

    /**
     * Creates a new instance that fetches data from the specified file.
     *
     * @param chunkSize the number of bytes to fetch on each
     *                  {@link #readChunk(ChannelHandlerContext)} call
     */
    public ChunkedIoUringFile(File in, int chunkSize, IoUringChunkedWriteHandler ioUringChunkedWriteHandler) throws IOException {
        this(new RandomAccessFile(in, "r").getChannel(), chunkSize, ioUringChunkedWriteHandler);
    }

    /**
     * Creates a new instance that fetches data from the specified file.
     */
    public ChunkedIoUringFile(FileChannel in, IoUringChunkedWriteHandler ioUringChunkedWriteHandler) throws IOException {
        this(in, DEFAULT_CHUNK_SIZE, ioUringChunkedWriteHandler);
    }

    /**
     * Creates a new instance that fetches data from the specified file.
     *
     * @param chunkSize the number of bytes to fetch on each
     *                  {@link #readChunk(ChannelHandlerContext)} call
     */
    public ChunkedIoUringFile(FileChannel in, int chunkSize, IoUringChunkedWriteHandler ioUringChunkedWriteHandler) throws IOException {
        this(in, 0, in.size(), chunkSize, ioUringChunkedWriteHandler);
    }

    /**
     * Creates a new instance that fetches data from the specified file.
     *
     * @param offset the offset of the file where the transfer begins
     * @param length the number of bytes to transfer
     * @param chunkSize the number of bytes to fetch on each
     *                  {@link #readChunk(ChannelHandlerContext)} call
     */
    public ChunkedIoUringFile(FileChannel in, long offset, long length, int chunkSize, IoUringChunkedWriteHandler ioUringChunkedWriteHandler)
            throws IOException {
        super(in, offset, length, chunkSize);
        this.ioUringChunkedWriteHandler = ioUringChunkedWriteHandler;
        this.fd = Native.getFd(in);
        this.ioUringAsyncFileIoHandle = new IoUringAsyncFileIoHandle();
        this.readBufferReference = new AtomicReference<>();
    }

    @Override
    public void close() throws Exception {
        IoRegistration ioRegistration = registration;
        if (ioRegistration != null) {
            ioRegistration.cancel();
        }
    }

    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        Future<IoRegistration> future = registerFuture;
        if (!future.isDone()) {
            future.addListener(new GenericFutureListener<Future<? super IoRegistration>>() {
                @Override
                public void operationComplete(Future<? super IoRegistration> future) throws Exception {
                    ioUringChunkedWriteHandler.resumeTransfer();
                }
            });
            return null;
        }

        if (cause != null) {
            ReferenceCountUtil.release(readBufferReference.get());
            throw cause;
        }

        ByteBuf readChunk = readBufferReference.getAndSet(null);
        if (readChunk != null) {
            return readChunk;
        }

        int chunkSize = (int) Math.min(this.chunkSize, endOffset - offset);
        ByteBuf buffer = allocator.buffer(0);
        if (!readBufferReference.compareAndSet(null, buffer)) {
            ReferenceCountUtil.safeRelease(buffer);
            return null;
        }
        // cas successfully
        // now we can malloc the buffer
        buffer.capacity(chunkSize);
        registration.submit(IoUringIoOps.newRead(fd, buffer.memoryAddress(), offset, chunkSize));
        return null;
    }

    void register(IoEventLoop ioEventLoop) {
        registerFuture = ioEventLoop.register(this.ioUringAsyncFileIoHandle);
    }

    class IoUringAsyncFileIoHandle implements IoUringIoHandle {

        @Override
        public void handle(IoRegistration registration, IoEvent ioEvent) {
            IoUringIoEvent uringIoEvent = (IoUringIoEvent) ioEvent;
            int res = uringIoEvent.res();
            if (res < 0) {
                cause = Errors.newIOException("io_uring read", res);
                return;
            }
            ByteBuf readBuffer = readBufferReference.get();
            readBuffer.writerIndex(readBuffer.writerIndex() + res);
            ioUringChunkedWriteHandler.resumeTransfer();
        }

        @Override
        public void close() throws Exception {
            in.close();
        }
    }
}
