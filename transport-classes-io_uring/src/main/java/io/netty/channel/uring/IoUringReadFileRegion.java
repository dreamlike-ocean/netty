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

import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

final class IoUringReadFileRegion implements FileRegion {

    final DefaultFileRegion fileRegion;
    private long readTransferred;
    private long transferred;

    IoUringReadFileRegion(DefaultFileRegion fileRegion) {
        this.fileRegion = fileRegion;
    }

    void open() throws IOException {
        fileRegion.open();
    }

    int fd() {
        return Native.getFd(fileRegion);
    }

    long fileOffset() {
        return position() + readTransferred();
    }

    long readTransferred() {
        return readTransferred;
    }

    void advanceReadTransferred(long amount) {
        readTransferred += amount;
    }

    void advanceTransferred(long amount) {
        transferred += amount;
    }

    @Override
    public long position() {
        return fileRegion.position();
    }

    @Override
    @Deprecated
    public long transfered() {
        return transferred;
    }

    @Override
    public long transferred() {
        return transferred;
    }

    @Override
    public long count() {
        return fileRegion.count();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileRegion retain() {
        fileRegion.retain();
        return this;
    }

    @Override
    public FileRegion retain(int increment) {
        fileRegion.retain(increment);
        return this;
    }

    @Override
    public FileRegion touch() {
        fileRegion.touch();
        return this;
    }

    @Override
    public FileRegion touch(Object hint) {
        fileRegion.touch(hint);
        return this;
    }

    @Override
    public int refCnt() {
        return fileRegion.refCnt();
    }

    @Override
    public boolean release() {
        return fileRegion.release();
    }

    @Override
    public boolean release(int decrement) {
        return fileRegion.release(decrement);
    }
}
