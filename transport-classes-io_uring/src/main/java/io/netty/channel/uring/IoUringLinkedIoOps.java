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
    private final IoUringIoOps[] ops;

    private IoUringLinkedIoOps(IoUringIoOps[] ops) {
        this.ops = ops;
    }

    /**
     * Create a new linked chain. The returned chain owns normalized copies of the supplied operations:
     * {@code IOSQE_LINK} is set on every operation except the last one and cleared on the last operation.
     *
     * @param ops   operations to submit as one linked chain.
     * @return      linked operations.
     */
    public static IoUringLinkedIoOps of(IoUringIoOps... ops) {
        if (ops == null) {
            throw new NullPointerException("ops");
        }
        if (ops.length < 2) {
            throw new IllegalArgumentException("linked ops must contain at least two operations; submit a single "
                    + "IoUringIoOps directly");
        }
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

            byte flags = normalizeLinkFlag(op.flags(), i == ops.length - 1);
            copy[i] = flags == op.flags() ? op : withFlags(op, flags);
        }
        return new IoUringLinkedIoOps(copy);
    }

    /**
     * Returns the operation identifier for the operation at {@code index}, derived from the identifier returned when this
     * linked chain was submitted.
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
        if (submittedId >= 0) {
            throw new IllegalArgumentException("submittedId is not a valid linked operation identifier");
        }
        long sequence = PendingOpMap.tokenSequence(submittedId);
        return PendingOpMap.token(sequence + index);
    }

    public int size() {
        return ops.length;
    }

    public IoUringIoOps op(int index) {
        return ops[index];
    }

    private static byte normalizeLinkFlag(byte flags, boolean last) {
        return last ? (byte) (flags & ~Native.IOSQE_LINK) : (byte) (flags | Native.IOSQE_LINK);
    }

    private static IoUringIoOps withFlags(IoUringIoOps op, byte flags) {
        return new IoUringIoOps(op.opcode(), flags, op.ioPrio(), op.fd(), op.union1(), op.union2(), op.len(),
                op.union3(), op.userData(), op.union4(), op.personality(), op.union5(), op.union6());
    }
}
