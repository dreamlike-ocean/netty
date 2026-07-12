/*
 * Copyright 2026 The Netty Project
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

import io.netty.channel.IoOps;

/**
 * A linked chain submitted as contiguous SQEs, with submit returning the first operation id
 */
public final class IoUringLinkedIoOps implements IoOps {
    private static final byte LINKED_FLAGS = (byte) (Native.IOSQE_LINK | Native.IOSQE_IO_HARDLINK);
    private final IoUringIoOps[] ops;

    private IoUringLinkedIoOps(IoUringIoOps[] ops) {
        this.ops = ops;
    }

    /**
     * Returns whether linked operation submission is supported.
     *
     * @return {@code true} if linked operation submission is supported.
     */
    public static boolean isSupported() {
        return IoUring.isSetupSubmitAllSupported();
    }

    /**
     * Create a new soft-linked chain. The returned chain owns normalized copies of the supplied operations:
     * {@code IOSQE_LINK} is set on every operation except the last one and all link flags are cleared on the last
     * operation.
     *
     * @param ops   operations to submit as one linked chain.
     * @return      linked operations.
     */
    public static IoUringLinkedIoOps of(IoUringIoOps... ops) {
        return of(false, ops);
    }

    /**
     * Create a new linked chain. The returned chain owns normalized copies of the supplied operations. Every operation
     * except the last one uses {@code IOSQE_IO_HARDLINK} when {@code hardLink} is {@code true}, or {@code IOSQE_LINK}
     * otherwise. All link flags are cleared on the last operation.
     *
     * @param hardLink  {@code true} to use hard links, {@code false} to use soft links.
     * @param ops       operations to submit as one linked chain.
     * @return          linked operations.
     */
    public static IoUringLinkedIoOps of(boolean hardLink, IoUringIoOps... ops) {
        if (!isSupported()) {
            throw new UnsupportedOperationException(
                    "IoUringLinkedIoOps requires IORING_SETUP_SUBMIT_ALL support");
        }
        if (ops == null) {
            throw new NullPointerException("ops");
        }
        if (ops.length < 2) {
            throw new IllegalArgumentException("linked ops must contain at least two operations; submit a single "
                    + "IoUringIoOps directly");
        }
        byte linkedFlag = (byte) (hardLink ? Native.IOSQE_IO_HARDLINK : Native.IOSQE_LINK);
        IoUringIoOps[] copy = new IoUringIoOps[ops.length];
        for (int i = 0; i < ops.length; i++) {
            IoUringIoOps op = ops[i];
            if (op == null) {
                throw new NullPointerException("ops[" + i + ']');
            }
            if ((op.flags() & Native.IOSQE_CQE_SKIP_SUCCESS) != 0) {
                throw new IllegalArgumentException("ops[" + i + "] uses IOSQE_CQE_SKIP_SUCCESS; linked ops require "
                        + "one terminal CQE per SQE so completions can be matched back to the original operation");
            }

            byte flags = normalizeLinkFlags(op.flags(), i == ops.length - 1, linkedFlag);
            copy[i] = flags == op.flags() ? op : withFlags(op, flags);
        }
        return new IoUringLinkedIoOps(copy);
    }

    /**
     * Returns the operation identifier for the operation at {@code index}, derived from the identifier returned when
     * this linked chain was submitted.
     * <p>
     * {@code submittedId} must be the value returned by {@link io.netty.channel.IoRegistration#submit(IoOps)} for this
     * {@link IoUringLinkedIoOps} instance.
     * <p>
     * The returned identifier can be used to refer to the individual operation at {@code index}.
     *
     * @param submittedId   the identifier returned when this linked chain was submitted.
     * @param index         the index of the operation in this linked chain.
     * @return              the identifier for the operation at {@code index}.
     */
    public long tokenAtIndex(long submittedId, int index) {
        if (index < 0 || index >= ops.length) {
            throw new IndexOutOfBoundsException("index=" + index + ", size=" + ops.length);
        }
        return PendingOpMap.tokenAtIndex(submittedId, index, ops.length);
    }

    public int size() {
        return ops.length;
    }

    public IoUringIoOps op(int index) {
        return ops[index];
    }

    private static byte normalizeLinkFlags(byte flags, boolean last, byte linkFlag) {
        flags &= (byte) ~LINKED_FLAGS;
        if (last) {
            return flags;
        }
        return (byte) (flags | linkFlag);
    }

    private static IoUringIoOps withFlags(IoUringIoOps op, byte flags) {
        return new IoUringIoOps(op.opcode(), flags, op.ioPrio(), op.fd(), op.union1(), op.union2(), op.len(),
                op.union3(), op.userData(), op.union4(), op.personality(), op.union5(), op.union6());
    }
}
