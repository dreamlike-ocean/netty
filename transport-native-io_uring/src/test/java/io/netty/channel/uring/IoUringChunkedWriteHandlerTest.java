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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.util.NetUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class IoUringChunkedWriteHandlerTest {
    private static final byte[] BYTES = new byte[1024 * 64];
    private static final File TMP;

    static {
        for (int i = 0; i < BYTES.length; i++) {
            BYTES[i] = (byte) i;
        }

        try {
            TMP = PlatformDependent.createTempFile("netty-iouring-chunk-", ".tmp", null);
            TMP.deleteOnExit();
            try (FileOutputStream out = new FileOutputStream(TMP)) {
                out.write(BYTES);
                out.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void loadJNI() {
        assumeTrue(IoUring.isAvailable());
    }

    @Test
    public void runOnEventLoop() throws Exception {
        test(true);
    }

    @Test
    public void runOnOtherThread() throws Exception {
        test(false);
    }

    public void test(boolean onEventLoop) throws Exception {
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        DefaultEventExecutorGroup eventExecutors = new DefaultEventExecutorGroup(1);
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.channel(IoUringServerSocketChannel.class);
            CompletableFuture<ByteBuf> recvAllByteFuture = new CompletableFuture<>();
            Channel serverChannel = serverBootstrap.group(group)
                    .childHandler(new ChannelInboundHandlerAdapter() {
                        private CompositeByteBuf compositeByteBuf;

                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                            if (compositeByteBuf == null) {
                                compositeByteBuf = ctx.alloc().compositeBuffer();
                            }
                            compositeByteBuf.addComponent(true, (ByteBuf) msg);
                            if (compositeByteBuf.readableBytes() >= BYTES.length) {
                                recvAllByteFuture.complete(compositeByteBuf);
                            }
                        }
                    })
                    .bind(NetUtil.LOCALHOST, 0)
                    .syncUninterruptibly().channel();
            Bootstrap clientBoostrap = new Bootstrap();
            clientBoostrap.group(group)
                    .channel(IoUringSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            if (onEventLoop) {
                                ch.pipeline().addLast(new IoUringChunkedWriteHandler());
                            } else {
                                ch.pipeline().addLast(eventExecutors, new IoUringChunkedWriteHandler());
                            }
                        }
                    });
            ChannelFuture channelFuture = clientBoostrap.connect(serverChannel.localAddress()).syncUninterruptibly();
            ChunkedIoUringFile msg = new ChunkedIoUringFile(TMP);
            channelFuture.channel().writeAndFlush(msg).syncUninterruptibly();
            ByteBuf byteBuf = recvAllByteFuture.get();
            assumeTrue(ByteBufUtil.equals(byteBuf, Unpooled.wrappedBuffer(BYTES)));
            assumeTrue(msg.isEndOfInput());
            byteBuf.release();
        } finally {
            group.shutdownGracefully(0, 15, TimeUnit.SECONDS);
            eventExecutors.shutdownGracefully(0, 15, TimeUnit.SECONDS);
        }
    }
}
