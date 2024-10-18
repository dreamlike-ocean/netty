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
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class IoUringFileTest {

    private static EventLoopGroup group;

    @BeforeAll
    public static void loadJNI() {
        assumeTrue(IoUring.isAvailable());
        group =  new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
    }

    @Test
    public void testGetFd() throws IOException {
        File file = File.createTempFile("temp", ".tmp");
        file.deleteOnExit();
        FileChannel channel = FileChannel.open(file.toPath());
        int fd = Native.getFd(channel);
        Assertions.assertTrue(fd > 0);
    }

    @Test
    public void testAsyncFdRead() throws IOException, ExecutionException, InterruptedException {
        EventLoop eventLoop = group.next();
        File file = File.createTempFile("temp", ".tmp");
        file.deleteOnExit();
        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        byte[] hellIoUring = "Hello Netty IoUring".getBytes(StandardCharsets.UTF_8);
        Files.write(file.toPath(), hellIoUring);

        IoUringAsyncFile uringAsyncFile = new IoUringAsyncFile(channel);
        uringAsyncFile.register(eventLoop).sync();

        ByteBuf buf = Unpooled.directBuffer(hellIoUring.length);
        IoUringAsyncFile.AsyncFileOperatorResult result = uringAsyncFile.read(buf, hellIoUring.length, 0).get();
        Assertions.assertEquals(hellIoUring.length, result.getResult());
        buf.release();

        buf = Unpooled.directBuffer(hellIoUring.length + 4);
        buf.writerIndex(4);
        result = uringAsyncFile.read(buf, hellIoUring.length, 0).sync().getNow();
        Assertions.assertEquals(hellIoUring.length, result.getResult());
        Assertions.assertEquals(hellIoUring.length + 4, buf.writerIndex());

        byte[] dst = new byte[hellIoUring.length];
        buf.readerIndex(4).readBytes(dst);
        Assertions.assertArrayEquals(hellIoUring, dst);

        buf.release();
    }

    @AfterAll
    public static void closeResource() throws InterruptedException {
        group.shutdownGracefully().sync();
    }
}
