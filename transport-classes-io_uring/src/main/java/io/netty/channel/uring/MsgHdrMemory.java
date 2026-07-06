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
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.unix.Buffer;
import io.netty.util.internal.CleanableDirectBuffer;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class MsgHdrMemory {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MsgHdrMemory.class);

    public static final int MSG_HDR_SIZE =
            Native.SIZEOF_MSGHDR + Native.SIZEOF_SOCKADDR_STORAGE + Native.SIZEOF_IOVEC + Native.CMSG_SPACE;
    private static final byte[] EMPTY_SOCKADDR_STORAGE = new byte[Native.SIZEOF_SOCKADDR_STORAGE];
    // It is not possible to have a zero length buffer in sendFd,
    // so we use a 1 byte buffer here.
    private static final int GLOBAL_IOV_LEN = 1;
    private static final ByteBuffer GLOBAL_IOV_BASE =  Buffer.allocateDirectWithNativeOrder(GLOBAL_IOV_LEN);
    private static final long GLOBAL_IOV_BASE_ADDRESS = Buffer.memoryAddress(GLOBAL_IOV_BASE);
    private final CleanableDirectBuffer msgHdrMemoryCleanable;
    private final CleanableDirectBuffer socketAddrMemoryCleanable;
    private final CleanableDirectBuffer iovMemoryCleanable;
    private final CleanableDirectBuffer cmsgDataMemoryCleanable;
    private final ByteBuffer msgHdrMemory;
    private final ByteBuffer socketAddrMemory;
    private final ByteBuffer iovMemory;
    private final ByteBuffer cmsgDataMemory;

    private final long msgHdrMemoryAddress;
    private final short idx;
    private final int cmsgDataOffset;

    MsgHdrMemory(short idx, ByteBuffer msgHdrMemoryArray) {
        this.idx = idx;
        this.msgHdrMemoryCleanable = null;
        this.socketAddrMemoryCleanable = null;
        this.iovMemoryCleanable = null;
        this.cmsgDataMemoryCleanable = null;
        int offset = idx * MSG_HDR_SIZE;
        // ByteBuffer.slice(int, int) / duplicate() are specified to produce BIG_ENDIAN byte buffers.
        // Set native order explicitly so native structs written via putInt/putLong use the expected endianness.
        this.msgHdrMemory = PlatformDependent.offsetSlice(
                msgHdrMemoryArray, offset, Native.SIZEOF_MSGHDR
        ).order(ByteOrder.nativeOrder());
        offset += Native.SIZEOF_MSGHDR;
        this.socketAddrMemory = PlatformDependent.offsetSlice(
                msgHdrMemoryArray, offset, Native.SIZEOF_SOCKADDR_STORAGE
        ).order(ByteOrder.nativeOrder());
        offset += Native.SIZEOF_SOCKADDR_STORAGE;
        this.iovMemory = PlatformDependent.offsetSlice(
                msgHdrMemoryArray, offset, Native.SIZEOF_IOVEC
        ).order(ByteOrder.nativeOrder());
        offset += Native.SIZEOF_IOVEC;
        this.cmsgDataMemory = PlatformDependent.offsetSlice(
                msgHdrMemoryArray, offset, Native.CMSG_SPACE
        ).order(ByteOrder.nativeOrder());

        msgHdrMemoryAddress = Buffer.memoryAddress(msgHdrMemory);

        long cmsgDataMemoryAddr = Buffer.memoryAddress(cmsgDataMemory);
        long cmsgDataAddr = Native.cmsghdrData(cmsgDataMemoryAddr);
        cmsgDataOffset = (int) (cmsgDataAddr - cmsgDataMemoryAddr);
    }

    MsgHdrMemory() {
        this.idx = 0;
        // jdk will memset the memory to 0, so we don't need to do it here.
        msgHdrMemoryCleanable = Buffer.allocateDirectBufferWithNativeOrder(Native.SIZEOF_MSGHDR);
        socketAddrMemoryCleanable = null;
        iovMemoryCleanable = Buffer.allocateDirectBufferWithNativeOrder(Native.SIZEOF_IOVEC);
        cmsgDataMemoryCleanable = Buffer.allocateDirectBufferWithNativeOrder(Native.CMSG_SPACE_FOR_FD);

        msgHdrMemory = msgHdrMemoryCleanable.buffer();
        socketAddrMemory = null;
        iovMemory = iovMemoryCleanable.buffer();
        cmsgDataMemory = cmsgDataMemoryCleanable.buffer();

        msgHdrMemoryAddress = Buffer.memoryAddress(msgHdrMemory);
        // These two parameters must be set to valid values and cannot be 0,
        // otherwise the fd we get in io_uring_recvmsg is 0
        Iov.set(iovMemory, GLOBAL_IOV_BASE_ADDRESS, GLOBAL_IOV_LEN);

        long cmsgDataMemoryAddr = Buffer.memoryAddress(cmsgDataMemory);
        long cmsgDataAddr = Native.cmsghdrData(cmsgDataMemoryAddr);
        cmsgDataOffset = (int) (cmsgDataAddr - cmsgDataMemoryAddr);
    }

    void set(LinuxSocket socket, InetSocketAddress address, long bufferAddress , int length, short segmentSize) {
        int addressLength = setSocketAddress(socket, address);
        Iov.set(iovMemory, bufferAddress, length);
        MsgHdr.set(msgHdrMemory, socketAddrMemory, addressLength, iovMemory, 1, cmsgDataMemory,
                cmsgDataOffset, segmentSize);
    }

    void setRecvmsgProviderBuffer(LinuxSocket socket, int length) {
        int addressLength = setSocketAddress(socket, null);
        Iov.set(iovMemory, 0, length);
        MsgHdr.set(msgHdrMemory, socketAddrMemory, addressLength, iovMemory, 1, cmsgDataMemory,
                cmsgDataOffset, (short) 0);
    }

    void setRecvmsgMultishotProviderBuffer(LinuxSocket socket) {
        int addressLength = setSocketAddress(socket, null);
        MsgHdr.set(msgHdrMemory, socketAddrMemory, addressLength, 0, 0, 0, 0);
    }

    void set(long iovArray, int length) {
        MsgHdr.set(msgHdrMemory, iovArray, length);
    }

    void setWithIovArrayAddress(LinuxSocket socket, InetSocketAddress address,
                                long iovArrayAddress, int iovArrayLength, short segmentSize) {
        int addressLength = setSocketAddress(socket, address);
        MsgHdr.set(msgHdrMemory, socketAddrMemory, addressLength, iovArrayAddress, iovArrayLength,
                cmsgDataMemory, cmsgDataOffset, segmentSize);
    }

    private int setSocketAddress(LinuxSocket socket, InetSocketAddress address) {
        int addressLength;
        if (address == null) {
            addressLength = socket.isIpv6() ? Native.SIZEOF_SOCKADDR_IN6 : Native.SIZEOF_SOCKADDR_IN;
            socketAddrMemory.mark();
            try {
                socketAddrMemory.put(EMPTY_SOCKADDR_STORAGE);
            } finally {
                socketAddrMemory.reset();
            }
        } else {
            addressLength = SockaddrIn.set(socket.isIpv6(), socketAddrMemory, address);
        }
        return addressLength;
    }

    void setScmRightsFd(int fd) {
        MsgHdr.prepSendFd(msgHdrMemory, fd, cmsgDataMemory, cmsgDataOffset, iovMemory, 1);
    }

    int getScmRightsFd() {
        return MsgHdr.getCmsgData(msgHdrMemory, cmsgDataMemory, cmsgDataOffset);
    }

    void prepRecvReadFd() {
        MsgHdr.prepReadFd(msgHdrMemory, cmsgDataMemory, cmsgDataOffset, iovMemory, 1);
    }

    boolean hasPort(IoUringDatagramChannel channel) {
        if (channel.socket.isIpv6()) {
            return SockaddrIn.hasPortIpv6(socketAddrMemory);
        }
        return SockaddrIn.hasPortIpv4(socketAddrMemory);
    }

    DatagramPacket get(IoUringDatagramChannel channel, IoUringIoHandler handler, ByteBuf buffer, int bytesRead) {
        InetSocketAddress sender = sender(channel, handler, socketAddrMemory);
        long bufferAddress = Iov.getBufferAddress(iovMemory);
        int bufferLength = Iov.getBufferLength(iovMemory);
        // reconstruct the reader index based on the memoryAddress of the buffer and the bufferAddress that was used
        // in the iovec.
        long memoryAddress = IoUring.memoryAddress(buffer);
        int readerIndex = (int) (bufferAddress - memoryAddress);

        ByteBuf slice = buffer.slice(readerIndex, bufferLength)
                .writerIndex(bytesRead);
        return new DatagramPacket(slice.retain(), channel.localAddress(), sender);
    }

    DatagramPacket getWithProviderBuffer(IoUringDatagramChannel channel, IoUringIoHandler handler, ByteBuf buffer) {
        InetSocketAddress sender = sender(channel, handler, socketAddrMemory);
        return new DatagramPacket(buffer, channel.localAddress(), sender);
    }

    DatagramPacket getWithMultishotProviderBuffer(
            IoUringDatagramChannel channel, IoUringIoHandler handler, ByteBuf buffer, int bytesRead) {
        int readerIndex = buffer.readerIndex();
        int msgNameLength = msgNameLength();
        int msgControlLength = msgControlLength();
        // Matches liburing's io_uring_recvmsg_validate / io_uring_recvmsg_payload:
        // https://github.com/axboe/liburing/blame/master/src/include/liburing.h#L1198-L1263
        int payloadOffset = Native.SIZEOF_IO_URING_RECVMSG_OUT + msgNameLength + msgControlLength;
        if (bytesRead < payloadOffset) {
            throw new IllegalStateException("io_uring recvmsg multishot metadata does not fit: payloadOffset=" +
                    payloadOffset + ", bytesRead=" + bytesRead);
        }

        int payloadLength = getIntNative(buffer, readerIndex + Native.IO_URING_RECVMSG_OUT_OFFSETOF_PAYLOADLEN);
        int msgFlags = getIntNative(buffer, readerIndex + Native.IO_URING_RECVMSG_OUT_OFFSETOF_FLAGS);

        InetSocketAddress sender = sender(channel, handler, buffer,
                readerIndex + Native.SIZEOF_IO_URING_RECVMSG_OUT);
        int availablePayloadLength = bytesRead - payloadOffset;
        if ((msgFlags & Native.MSG_TRUNC) != 0 || payloadLength > availablePayloadLength) {
            if (logger.isDebugEnabled()) {
                logger.debug("{} io_uring recvmsg multishot truncated datagram, consider increasing the buffer ring "
                                + "chunk size: payloadLength={}, availablePayloadLength={}, bytesRead={}, msgFlags={}",
                        channel, payloadLength, availablePayloadLength, bytesRead, msgFlags);
            }
            payloadLength = Math.min(payloadLength, availablePayloadLength);
        }
        buffer.readerIndex(readerIndex + payloadOffset);
        buffer.writerIndex(buffer.readerIndex() + payloadLength);
        return new DatagramPacket(buffer, channel.localAddress(), sender);
    }

    private int msgNameLength() {
        return msgHdrMemory.getInt(msgHdrMemory.position() + Native.MSGHDR_OFFSETOF_MSG_NAMELEN);
    }

    private int msgControlLength() {
        int offset = msgHdrMemory.position() + Native.MSGHDR_OFFSETOF_MSG_CONTROLLEN;
        if (Native.SIZEOF_SIZE_T == 4) {
            return msgHdrMemory.getInt(offset);
        }
        assert Native.SIZEOF_SIZE_T == 8;
        return (int) msgHdrMemory.getLong(offset);
    }

    private static int getIntNative(ByteBuf buffer, int index) {
        return PlatformDependent.BIG_ENDIAN_NATIVE_ORDER ? buffer.getInt(index) : buffer.getIntLE(index);
    }

    private static InetSocketAddress sender(
            IoUringDatagramChannel channel, IoUringIoHandler handler, ByteBuffer socketAddrMemory) {
        InetSocketAddress sender;
        if (channel.socket.isIpv6()) {
            byte[] ipv6Bytes = handler.inet6AddressArray();
            byte[] ipv4bytes = handler.inet4AddressArray();

            sender = SockaddrIn.getIPv6(socketAddrMemory, ipv6Bytes, ipv4bytes);
        } else {
            byte[] bytes = handler.inet4AddressArray();
            sender = SockaddrIn.getIPv4(socketAddrMemory, bytes);
        }
        return sender;
    }

    private static InetSocketAddress sender(
            IoUringDatagramChannel channel, IoUringIoHandler handler, ByteBuf buffer, int socketAddrIndex) {
        if (channel.socket.isIpv6()) {
            byte[] ipv6Bytes = handler.inet6AddressArray();
            byte[] ipv4Bytes = handler.inet4AddressArray();

            int port = buffer.getShort(socketAddrIndex + Native.SOCKADDR_IN6_OFFSETOF_SIN6_PORT) & 0xFFFF;
            int addressIndex = socketAddrIndex +
                    Native.SOCKADDR_IN6_OFFSETOF_SIN6_ADDR + Native.IN6_ADDRESS_OFFSETOF_S6_ADDR;
            buffer.getBytes(addressIndex, ipv6Bytes);
            try {
                if (PlatformDependent.equals(
                        ipv6Bytes, 0, SockaddrIn.IPV4_MAPPED_IPV6_PREFIX, 0,
                        SockaddrIn.IPV4_MAPPED_IPV6_PREFIX.length)) {
                    System.arraycopy(ipv6Bytes, SockaddrIn.IPV4_MAPPED_IPV6_PREFIX.length,
                            ipv4Bytes, 0, SockaddrIn.IPV4_ADDRESS_LENGTH);
                    return new InetSocketAddress(Inet4Address.getByAddress(ipv4Bytes), port);
                }
                int scopeId = getIntNative(buffer, socketAddrIndex + Native.SOCKADDR_IN6_OFFSETOF_SIN6_SCOPE_ID);
                return new InetSocketAddress(Inet6Address.getByAddress(null, ipv6Bytes, scopeId), port);
            } catch (UnknownHostException ignore) {
                return null;
            }
        }

        byte[] bytes = handler.inet4AddressArray();
        int port = buffer.getShort(socketAddrIndex + Native.SOCKADDR_IN_OFFSETOF_SIN_PORT) & 0xFFFF;
        int addressIndex = socketAddrIndex + Native.SOCKADDR_IN_OFFSETOF_SIN_ADDR + Native.IN_ADDRESS_OFFSETOF_S_ADDR;
        buffer.getBytes(addressIndex, bytes);
        try {
            return new InetSocketAddress(InetAddress.getByAddress(bytes), port);
        } catch (UnknownHostException ignore) {
            return null;
        }
    }

    short idx() {
        return idx;
    }

    long address() {
        return msgHdrMemoryAddress;
    }

    void release() {
        if (msgHdrMemoryCleanable != null) {
            msgHdrMemoryCleanable.clean();
        }
        if (socketAddrMemoryCleanable != null) {
            socketAddrMemoryCleanable.clean();
        }
        if (iovMemoryCleanable != null) {
            iovMemoryCleanable.clean();
        }
        if (cmsgDataMemoryCleanable != null) {
            cmsgDataMemoryCleanable.clean();
        }
    }
}
