/*
 * Copyright 2012 The Netty Project
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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.util.NetUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class IoUringProviderBufferReadTest {
    @BeforeAll
    public static void loadJNI() {
        assumeTrue(IoUring.isAvailable());
    }

    private BlockingQueue<ByteBuf> bufferSyncer = new LinkedBlockingQueue<>();

    @Test
    public void testProviderBufferRead() throws InterruptedException {
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.channel(IoUringServerSocketChannel.class);

        String randomString = UUID.randomUUID().toString();
        int randomStringLength = randomString.length();

        Channel serverChannel = serverBootstrap.group(group)
                .childHandler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        bufferSyncer.offer((ByteBuf) msg);
                    }
                })
                .childOption(IoUringChannelOption.ENABLE_PROVIDER_BUFFER_READ, true)
                .childOption(
                        IoUringChannelOption.IOURING_BUFFER_RING_CONFIG,
                        new BufferRingConfig((short) 1, (short) 2, 1024)
                )
                .bind(NetUtil.LOCALHOST, 0)
                .syncUninterruptibly().channel();

        Bootstrap clientBoostrap = new Bootstrap();
        clientBoostrap.group(group)
                .channel(IoUringSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter());
        Channel clientChannel = clientBoostrap.connect(serverChannel.localAddress()).syncUninterruptibly().channel();

        //is provider buffer read?
        ByteBuf writeBuffer = Unpooled.directBuffer(randomStringLength);
        writeBuffer.retain();
        ByteBufUtil.writeAscii(writeBuffer, randomString);
        ByteBuf readBuffer = sendAndRecvMessage(clientChannel, writeBuffer);
        assertInstanceOf(IoUringBufferRing.UserspaceIoUringBuffer.class, readBuffer);
        ByteBuf userspaceIoUringBufferElement1 = readBuffer;
        ByteBuf userspaceIoUringBufferElement2 = sendAndRecvMessage(clientChannel, writeBuffer);
        assertInstanceOf(IoUringBufferRing.UserspaceIoUringBuffer.class, readBuffer);

        //now we run out of buffer ring buffer
        readBuffer = sendAndRecvMessage(clientChannel, writeBuffer);
        assertFalse(readBuffer instanceof IoUringBufferRing.UserspaceIoUringBuffer);

        //now we release the buffer ring buffer
        userspaceIoUringBufferElement1.release();
        readBuffer = sendAndRecvMessage(clientChannel, writeBuffer);
        assertInstanceOf(IoUringBufferRing.UserspaceIoUringBuffer.class, readBuffer);

        writeBuffer.release();
        serverChannel.close();
        clientChannel.close();
        group.shutdownGracefully();
    }

    public ByteBuf sendAndRecvMessage(Channel clientChannel, ByteBuf writeBuffer) throws InterruptedException {
        //retain the buffer to assert
        writeBuffer.retain();
        clientChannel.writeAndFlush(writeBuffer).sync();
        ByteBuf readBuffer = bufferSyncer.take();
        assertEquals(writeBuffer.readableBytes(), readBuffer.readableBytes());
        assertTrue(ByteBufUtil.equals(writeBuffer, readBuffer));
        return readBuffer;
    }
}
