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
import io.netty.buffer.WrappedByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.NetUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class IoUringBufferRingTest {
    @BeforeAll
    public static void loadJNI() {
        assumeTrue(IoUring.isAvailable());
        assumeTrue(IoUring.isRegisterBufferRingSupported());
    }

    @Test
    public void testRegister() {
        // using cqeSize on purpose NOT a power of 2
        RingBuffer ringBuffer = Native.createRingBuffer(8, 15, 0);
        try {
            int ringFd = ringBuffer.fd();
            long ioUringBufRingAddr = Native.ioUringRegisterBufRing(ringFd, 4, (short) 1, 0);
            assumeTrue(
                    ioUringBufRingAddr > 0,
                    "ioUringSetupBufRing result must great than 0, but now result is " + ioUringBufRingAddr);
            int freeRes = Native.ioUringUnRegisterBufRing(ringFd, ioUringBufRingAddr, 4, (short) 1);
            assertEquals(
                    0,
                    freeRes,
                    "ioUringFreeBufRing result must be 0, but now result is " + freeRes
            );
            // let io_uring to "fix" it
            assertEquals(16, ringBuffer.ioUringCompletionQueue().ringCapacity);
        } finally {
            ringBuffer.close();
        }
    }

    private static ByteBuf unwrapLeakAware(ByteBuf buf) {
        // If its a sub-type of WrappedByteBuf we know its because it was wrapped for leak-detection.
        if (buf instanceof WrappedByteBuf) {
            return buf.unwrap();
        }
        return buf;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testProviderBufferRead(boolean incremental) throws InterruptedException {
        if (incremental) {
            assumeTrue(IoUring.isRegisterBufferRingIncSupported());
        }
        final BlockingQueue<ByteBuf> bufferSyncer = new LinkedBlockingQueue<>();
        IoUringIoHandlerConfig ioUringIoHandlerConfiguration = new IoUringIoHandlerConfig();
        IoUringBufferRingConfig bufferRingConfig =
                IoUringBufferRingConfig.builder()
                        .bufferGroupId((short) 1)
                        .bufferRingSize((short) 2)
                        .batchSize(2).incremental(incremental)
                        .allocator(new IoUringFixedBufferRingAllocator(1024))
                        .batchAllocation(false)
                        .build();

        IoUringBufferRingConfig bufferRingConfig1 =
                IoUringBufferRingConfig.builder()
                        .bufferGroupId((short) 2)
                        .bufferRingSize((short) 16)
                        .batchSize(8)
                        .incremental(incremental)
                        .allocator(new IoUringFixedBufferRingAllocator(1024))
                        .batchAllocation(true)
                        .build();
        ioUringIoHandlerConfiguration.setBufferRingConfig(bufferRingConfig, bufferRingConfig1);

        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1,
                IoUringIoHandler.newFactory(ioUringIoHandlerConfiguration)
        );
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.channel(IoUringServerSocketChannel.class);

        String randomString = UUID.randomUUID().toString();
        int randomStringLength = randomString.length();

        ArrayBlockingQueue<IoUringBufferRingExhaustedEvent> eventSyncer = new ArrayBlockingQueue<>(1);

        Channel serverChannel = serverBootstrap.group(group)
                .childHandler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        bufferSyncer.offer((ByteBuf) msg);
                    }

                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                        if (evt instanceof IoUringBufferRingExhaustedEvent) {
                            eventSyncer.add((IoUringBufferRingExhaustedEvent) evt);
                        }
                    }
                })
                .childOption(IoUringChannelOption.IO_URING_BUFFER_GROUP_ID, bufferRingConfig.bufferGroupId())
                .bind(NetUtil.LOCALHOST, 0)
                .syncUninterruptibly().channel();

        Bootstrap clientBoostrap = new Bootstrap();
        clientBoostrap.group(group)
                .channel(IoUringSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter());
        ChannelFuture channelFuture = clientBoostrap.connect(serverChannel.localAddress()).syncUninterruptibly();
        assumeTrue(channelFuture.isSuccess());
        Channel clientChannel = channelFuture.channel();

        //is provider buffer read?
        ByteBuf writeBuffer = Unpooled.directBuffer(randomStringLength);
        ByteBufUtil.writeAscii(writeBuffer, randomString);
        ByteBuf userspaceIoUringBufferElement1 = sendAndRecvMessage(clientChannel, writeBuffer, bufferSyncer);
        ByteBuf userspaceIoUringBufferElement2 = sendAndRecvMessage(clientChannel, writeBuffer, bufferSyncer);
        ByteBuf readBuffer = sendAndRecvMessage(clientChannel, writeBuffer, bufferSyncer);
        readBuffer.release();

        // Now we release the buffer and so put it back into the buffer ring.
        userspaceIoUringBufferElement1.release();
        userspaceIoUringBufferElement2.release();

        readBuffer = sendAndRecvMessage(clientChannel, writeBuffer, bufferSyncer);
        readBuffer.release();

        // The next buffer is expected to be provided out of the ring again.
        readBuffer = sendAndRecvMessage(clientChannel, writeBuffer, bufferSyncer);
        readBuffer.release();

        writeBuffer.release();

        serverChannel.close().syncUninterruptibly();
        clientChannel.close().syncUninterruptibly();
        group.shutdownGracefully();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDatagramProviderBufferRead(boolean incremental) throws InterruptedException {
        if (incremental) {
            assumeTrue(IoUring.isRegisterBufferRingIncSupported());
        }
        final BlockingQueue<DatagramPacket> packets = new LinkedBlockingQueue<>();
        final BlockingQueue<Throwable> exceptions = new LinkedBlockingQueue<>();
        IoUringIoHandlerConfig ioUringIoHandlerConfiguration = new IoUringIoHandlerConfig();
        IoUringBufferRingConfig bufferRingConfig =
                IoUringBufferRingConfig.builder()
                        .bufferGroupId((short) 1)
                        .bufferRingSize((short) 16)
                        .batchSize(8)
                        .incremental(incremental)
                        .allocator(new IoUringFixedBufferRingAllocator(1024))
                        .batchAllocation(false)
                        .build();
        ioUringIoHandlerConfiguration.setBufferRingConfig(bufferRingConfig);

        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1,
                IoUringIoHandler.newFactory(ioUringIoHandlerConfiguration)
        );
        Channel serverChannel = null;
        Channel clientChannel = null;
        try {
            Bootstrap serverBootstrap = new Bootstrap();
            serverChannel = serverBootstrap.group(group)
                    .channel(IoUringDatagramChannel.class)
                    .handler(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            packets.add((DatagramPacket) msg);
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            exceptions.add(cause);
                        }
                    })
                    .option(IoUringChannelOption.IO_URING_BUFFER_GROUP_ID, bufferRingConfig.bufferGroupId())
                    .option(IoUringChannelOption.MAX_DATAGRAM_PAYLOAD_SIZE, 0)
                    .bind(NetUtil.LOCALHOST, 0)
                    .syncUninterruptibly().channel();

            Bootstrap clientBootstrap = new Bootstrap();
            clientChannel = clientBootstrap.group(group)
                    .channel(IoUringDatagramChannel.class)
                    .handler(new ChannelInboundHandlerAdapter())
                    .bind(NetUtil.LOCALHOST, 0)
                    .syncUninterruptibly().channel();

            InetSocketAddress recipient = (InetSocketAddress) serverChannel.localAddress();
            sendAndRecvDatagram(clientChannel, recipient, "netty", packets, exceptions);
            sendAndRecvDatagram(clientChannel, recipient, "", packets, exceptions);
            sendAndRecvDatagram(clientChannel, recipient, "io_uring", packets, exceptions);
            sendAndRecvDatagram(clientChannel, recipient, "provider-buffer", packets, exceptions);
        } finally {
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
            if (clientChannel != null) {
                clientChannel.close().syncUninterruptibly();
            }
            DatagramPacket packet;
            while ((packet = packets.poll()) != null) {
                packet.release();
            }
            group.shutdownGracefully();
        }
    }

    @Test
    public void testDatagramProviderBufferReadFailsIfMultishotMetadataDoesNotFit() throws InterruptedException {
        assumeTrue(IoUring.isRecvMultishotEnabled());
        assumeTrue(IoUring.isRegisterBufferRingIncSupported());
        assertDatagramProviderBufferReadFailure(Integer.BYTES * 4 - 1, "netty", "too small");
    }

    @Test
    public void testDatagramProviderBufferReadDeliversTruncatedMultishotPayload() throws InterruptedException {
        assumeTrue(IoUring.isRecvMultishotEnabled());
        assumeTrue(IoUring.isRegisterBufferRingIncSupported());
        assertDatagramProviderBufferReadTruncated(64, asciiString(128));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDatagramProviderBufferReadSwitchesToRecvmsgBatching(boolean incremental)
            throws InterruptedException {
        if (incremental) {
            assumeTrue(IoUring.isRegisterBufferRingIncSupported());
        }
        final BlockingQueue<DatagramPacket> packets = new LinkedBlockingQueue<>();
        final BlockingQueue<Throwable> exceptions = new LinkedBlockingQueue<>();
        IoUringIoHandlerConfig ioUringIoHandlerConfiguration = new IoUringIoHandlerConfig();
        IoUringBufferRingConfig bufferRingConfig =
                IoUringBufferRingConfig.builder()
                        .bufferGroupId((short) 1)
                        .bufferRingSize((short) 16)
                        .batchSize(8)
                        .incremental(incremental)
                        .allocator(new IoUringFixedBufferRingAllocator(96))
                        .batchAllocation(false)
                        .build();
        ioUringIoHandlerConfiguration.setBufferRingConfig(bufferRingConfig);

        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1,
                IoUringIoHandler.newFactory(ioUringIoHandlerConfiguration)
        );
        Channel serverChannel = null;
        Channel clientChannel = null;
        try {
            Bootstrap serverBootstrap = new Bootstrap();
            serverChannel = serverBootstrap.group(group)
                    .channel(IoUringDatagramChannel.class)
                    .handler(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            packets.add((DatagramPacket) msg);
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            exceptions.add(cause);
                        }
                    })
                    .option(IoUringChannelOption.IO_URING_BUFFER_GROUP_ID, bufferRingConfig.bufferGroupId())
                    .option(IoUringChannelOption.MAX_DATAGRAM_PAYLOAD_SIZE, 0)
                    .bind(NetUtil.LOCALHOST, 0)
                    .syncUninterruptibly().channel();

            Bootstrap clientBootstrap = new Bootstrap();
            clientChannel = clientBootstrap.group(group)
                    .channel(IoUringDatagramChannel.class)
                    .handler(new ChannelInboundHandlerAdapter())
                    .bind(NetUtil.LOCALHOST, 0)
                    .syncUninterruptibly().channel();

            InetSocketAddress recipient = (InetSocketAddress) serverChannel.localAddress();
            sendAndRecvDatagram(clientChannel, recipient, "netty", packets, exceptions);

            final Channel server = serverChannel;
            server.eventLoop().submit(() ->
                    server.config().setOption(IoUringChannelOption.MAX_DATAGRAM_PAYLOAD_SIZE, 512))
                    .syncUninterruptibly();
            server.eventLoop().schedule(() -> { }, 50, TimeUnit.MILLISECONDS).syncUninterruptibly();

            sendAndRecvDatagram(clientChannel, recipient, asciiString(256), packets, exceptions);
        } finally {
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
            if (clientChannel != null) {
                clientChannel.close().syncUninterruptibly();
            }
            DatagramPacket packet;
            while ((packet = packets.poll()) != null) {
                packet.release();
            }
            group.shutdownGracefully();
        }
    }

    private static void assertDatagramProviderBufferReadFailure(int bufferSize, String message,
                                                               String expectedMessagePart)
            throws InterruptedException {
        assertDatagramProviderBufferRead(bufferSize, message, expectedMessagePart);
    }

    private static void assertDatagramProviderBufferReadTruncated(int bufferSize, String message)
            throws InterruptedException {
        assertDatagramProviderBufferRead(bufferSize, message, null);
    }

    private static void assertDatagramProviderBufferRead(int bufferSize, String message, String expectedMessagePart)
            throws InterruptedException {
        final BlockingQueue<DatagramPacket> packets = new LinkedBlockingQueue<>();
        final BlockingQueue<Throwable> exceptions = new LinkedBlockingQueue<>();
        IoUringIoHandlerConfig ioUringIoHandlerConfiguration = new IoUringIoHandlerConfig();
        IoUringBufferRingConfig bufferRingConfig =
                IoUringBufferRingConfig.builder()
                        .bufferGroupId((short) 1)
                        .bufferRingSize((short) 16)
                        .batchSize(8)
                        .incremental(true)
                        .allocator(new IoUringFixedBufferRingAllocator(bufferSize))
                        .batchAllocation(false)
                        .build();
        ioUringIoHandlerConfiguration.setBufferRingConfig(bufferRingConfig);

        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1,
                IoUringIoHandler.newFactory(ioUringIoHandlerConfiguration)
        );
        Channel serverChannel = null;
        Channel clientChannel = null;
        ByteBuf writeBuffer = Unpooled.directBuffer(message.length());
        ByteBuf expected = Unpooled.directBuffer(message.length());
        try {
            Bootstrap serverBootstrap = new Bootstrap();
            serverChannel = serverBootstrap.group(group)
                    .channel(IoUringDatagramChannel.class)
                    .handler(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            packets.add((DatagramPacket) msg);
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            exceptions.add(cause);
                        }
                    })
                    .option(IoUringChannelOption.IO_URING_BUFFER_GROUP_ID, bufferRingConfig.bufferGroupId())
                    .option(IoUringChannelOption.MAX_DATAGRAM_PAYLOAD_SIZE, 0)
                    .bind(NetUtil.LOCALHOST, 0)
                    .syncUninterruptibly().channel();

            Bootstrap clientBootstrap = new Bootstrap();
            clientChannel = clientBootstrap.group(group)
                    .channel(IoUringDatagramChannel.class)
                    .handler(new ChannelInboundHandlerAdapter())
                    .bind(NetUtil.LOCALHOST, 0)
                    .syncUninterruptibly().channel();

            ByteBufUtil.writeAscii(writeBuffer, message);
            ByteBufUtil.writeAscii(expected, message);
            InetSocketAddress recipient = (InetSocketAddress) serverChannel.localAddress();
            clientChannel.writeAndFlush(new DatagramPacket(writeBuffer.retainedDuplicate(), recipient))
                    .syncUninterruptibly();

            if (expectedMessagePart == null) {
                DatagramPacket packet = packets.poll(10, TimeUnit.SECONDS);
                Throwable cause = exceptions.poll();
                if (cause != null) {
                    throw new AssertionError(cause);
                }
                assertNotNull(packet);
                try {
                    int readableBytes = packet.content().readableBytes();
                    assertTrue(readableBytes > 0);
                    assertTrue(readableBytes < expected.readableBytes());
                    expected.writerIndex(readableBytes);
                    assertTrue(ByteBufUtil.equals(expected, packet.content()));
                } finally {
                    packet.release();
                }
            } else {
                Throwable cause = exceptions.poll(10, TimeUnit.SECONDS);
                assertNotNull(cause);
                assertTrue(cause instanceof IllegalStateException, cause.toString());
                assertTrue(cause.getMessage().contains(expectedMessagePart), cause.getMessage());
                assertTrue(packets.isEmpty());
            }
        } finally {
            writeBuffer.release();
            expected.release();
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
            if (clientChannel != null) {
                clientChannel.close().syncUninterruptibly();
            }
            DatagramPacket packet;
            while ((packet = packets.poll()) != null) {
                packet.release();
            }
            group.shutdownGracefully();
        }
    }

    static boolean recvsendBundleEnabled() {
        return IoUring.isRecvsendBundleEnabled();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @EnabledIf("recvsendBundleEnabled")
    public void testProviderBufferReadWithRecvsendBundle(boolean incremental) throws InterruptedException {
        // See https://lore.kernel.org/io-uring/184f9f92-a682-4205-a15d-89e18f664502@kernel.dk/T/#u
        assumeTrue(IoUring.isRecvMultishotEnabled(),
                "Only yields expected test results when using multishot atm");
        if (incremental) {
            assumeTrue(IoUring.isRegisterBufferRingIncSupported());
        }
        int bufferRingChunkSize = 8;
        IoUringIoHandlerConfig ioUringIoHandlerConfiguration = new IoUringIoHandlerConfig();
        IoUringBufferRingConfig bufferRingConfig = new IoUringBufferRingConfig(
                // let's use a small chunkSize so we are sure a recv will span multiple buffers.
                (short) 1, (short) 16, 8, 16 * 16,
                incremental, new IoUringFixedBufferRingAllocator(bufferRingChunkSize));

        ioUringIoHandlerConfiguration.setBufferRingConfig(bufferRingConfig);

        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1,
                IoUringIoHandler.newFactory(ioUringIoHandlerConfiguration)
        );
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.channel(IoUringServerSocketChannel.class);

        final BlockingQueue<ByteBuf> buffers = new LinkedBlockingQueue<>();
        Channel serverChannel = serverBootstrap.group(group)
                .childHandler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        buffers.offer((ByteBuf) msg);
                    }
                })
                .childOption(IoUringChannelOption.IO_URING_BUFFER_GROUP_ID, (short) 1)
                .bind(new InetSocketAddress(0))
                .syncUninterruptibly().channel();

        Bootstrap clientBoostrap = new Bootstrap();
        clientBoostrap.group(group)
                .channel(IoUringSocketChannel.class)
                .handler(new ChannelInboundHandlerAdapter());
        ChannelFuture channelFuture = clientBoostrap.connect(serverChannel.localAddress()).syncUninterruptibly();
        assumeTrue(channelFuture.isSuccess());
        Channel clientChannel = channelFuture.channel();

        // Create a buffer that will span multiple buffers that are used out of the buffer ring.
        ByteBuf writeBuffer = Unpooled.directBuffer(bufferRingChunkSize * 16);
        CompositeByteBuf received = Unpooled.compositeBuffer();
        try {
            // Fill the buffer with something so we can assert if the received bytes are the same.
            for (int i = 0; i < writeBuffer.capacity(); i++) {
                writeBuffer.writeByte((byte) i);
            }
            clientChannel.writeAndFlush(writeBuffer.retainedDuplicate()).syncUninterruptibly();

            // Aggregate all received buffers until we received everything.
            do {
                ByteBuf buffer = buffers.take();
                received.addComponent(true, buffer);
            } while (received.readableBytes() != writeBuffer.readableBytes());

            assertEquals(writeBuffer, received);
            serverChannel.close().syncUninterruptibly();
            clientChannel.close().syncUninterruptibly();
            group.shutdownGracefully();
            assertTrue(buffers.isEmpty());
        } finally {
            writeBuffer.release();
            received.release();
        }
    }

    private ByteBuf sendAndRecvMessage(Channel clientChannel, ByteBuf writeBuffer, BlockingQueue<ByteBuf> bufferSyncer)
            throws InterruptedException {
        //retain the buffer to assert
        clientChannel.writeAndFlush(writeBuffer.retainedDuplicate()).sync();
        ByteBuf readBuffer = bufferSyncer.take();
        assertEquals(writeBuffer.readableBytes(), readBuffer.readableBytes());
        assertTrue(ByteBufUtil.equals(writeBuffer, readBuffer));
        return readBuffer;
    }

    private void sendAndRecvDatagram(Channel clientChannel, InetSocketAddress recipient, String message,
                                     BlockingQueue<DatagramPacket> packets, BlockingQueue<Throwable> exceptions)
            throws InterruptedException {
        ByteBuf writeBuffer = Unpooled.directBuffer(message.length());
        ByteBuf expected = Unpooled.directBuffer(message.length());
        try {
            ByteBufUtil.writeAscii(writeBuffer, message);
            ByteBufUtil.writeAscii(expected, message);
            clientChannel.writeAndFlush(new DatagramPacket(writeBuffer.retainedDuplicate(), recipient))
                    .syncUninterruptibly();
            Throwable cause = exceptions.poll();
            if (cause != null) {
                throw new AssertionError(cause);
            }
            DatagramPacket packet = packets.poll(10, TimeUnit.SECONDS);
            cause = exceptions.poll();
            if (cause != null) {
                throw new AssertionError(cause);
            }
            assertNotNull(packet);
            try {
                assertEquals(expected.readableBytes(), packet.content().readableBytes());
                assertTrue(ByteBufUtil.equals(expected, packet.content()));
            } finally {
                packet.release();
            }
        } finally {
            writeBuffer.release();
            expected.release();
        }
    }

    private static String asciiString(int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append((char) ('a' + i % 26));
        }
        return builder.toString();
    }

    @Test
    public void testCloseEventLoopGroupWhileConnected() throws Exception {
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1,
                IoUringIoHandler.newFactory()
        );
        try {
            final BlockingQueue<Channel> acceptedChannels = new LinkedBlockingQueue<>();
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.channel(IoUringServerSocketChannel.class);
            Channel serverChannel = serverBootstrap.group(group)
                    .childHandler(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelActive(ChannelHandlerContext ctx) {
                            acceptedChannels.add(ctx.channel());
                        }
                    })
                    .bind(new InetSocketAddress(0))
                    .syncUninterruptibly().channel();

            Bootstrap clientBoostrap = new Bootstrap();
            clientBoostrap.group(group)
                    .channel(IoUringSocketChannel.class)
                    .handler(new ChannelInboundHandlerAdapter());
            ChannelFuture channelFuture = clientBoostrap.connect(serverChannel.localAddress());
            Channel clientChannel = channelFuture.sync().channel();

            group.shutdownGracefully().syncUninterruptibly();
            clientChannel.closeFuture().sync();
            serverChannel.closeFuture().sync();
            acceptedChannels.take().closeFuture().sync();
            assertTrue(acceptedChannels.isEmpty());
        } catch (Throwable t) {
            if (!group.isShutdown()) {
                group.shutdownGracefully().syncUninterruptibly();
            }
            throw t;
        }
    }
}
