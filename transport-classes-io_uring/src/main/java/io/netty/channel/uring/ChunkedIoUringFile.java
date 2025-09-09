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

    final int fd;
    private IoUringChunkedWriteHandler ioUringChunkedWriteHandler;
    private ByteBuf readBuffer;
    private Exception cause;

    /**
     * Creates a new instance that fetches data from the specified file.
     */
    public ChunkedIoUringFile(File in) throws IOException {
        this(new RandomAccessFile(in, "r").getChannel());
    }

    /**
     * Creates a new instance that fetches data from the specified file.
     *
     * @param chunkSize the number of bytes to fetch on each
     *                  {@link #readChunk(ChannelHandlerContext)} call
     */
    public ChunkedIoUringFile(File in, int chunkSize) throws IOException {
        this(new RandomAccessFile(in, "r").getChannel(), chunkSize);
    }

    /**
     * Creates a new instance that fetches data from the specified file.
     */
    public ChunkedIoUringFile(FileChannel in) throws IOException {
        this(in, DEFAULT_CHUNK_SIZE);
    }

    /**
     * Creates a new instance that fetches data from the specified file.
     *
     * @param chunkSize the number of bytes to fetch on each
     *                  {@link #readChunk(ChannelHandlerContext)} call
     */
    public ChunkedIoUringFile(FileChannel in, int chunkSize) throws IOException {
        this(in, 0, in.size(), chunkSize);
    }

    /**
     * Creates a new instance that fetches data from the specified file.
     *
     * @param offset the offset of the file where the transfer begins
     * @param length the number of bytes to transfer
     * @param chunkSize the number of bytes to fetch on each
     *                  {@link #readChunk(ChannelHandlerContext)} call
     */
    public ChunkedIoUringFile(FileChannel in, long offset, long length, int chunkSize) throws IOException {
        super(in, offset, length, chunkSize);
        this.fd = Native.getFd(in);
    }

    void handleFailed(Exception cause) {
        this.cause = cause;
        ioUringChunkedWriteHandler.resumeTransfer();
    }

    void handleSuccess(int readBytes) {
        this.offset += readBytes;
        ByteBuf readBuffer = this.readBuffer;
        readBuffer.writerIndex(readBuffer.writerIndex() + readBytes);
        ioUringChunkedWriteHandler.resumeTransfer();
    }

    void attach(IoUringChunkedWriteHandler ioUringChunkedWriteHandler) {
        this.ioUringChunkedWriteHandler = ioUringChunkedWriteHandler;
    }

    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {

        if (ioUringChunkedWriteHandler == null) {
            throw new IllegalArgumentException("Only IoUringChunkedWriteHandler can handle ChunkedIoUringFile");
        }

        if (cause != null) {
            ReferenceCountUtil.release(readBuffer);
            throw cause;
        }

        ByteBuf readBuffer = this.readBuffer;
        if (readBuffer != null) {
            this.readBuffer = null;
            return readBuffer;
        }

        int chunkSize = (int) Math.min(this.chunkSize, endOffset - offset);
        readBuffer = allocator.directBuffer(chunkSize);
        this.readBuffer = readBuffer;
        ioUringChunkedWriteHandler.requestAsyncRead(this, readBuffer);
        return null;
    }

}
