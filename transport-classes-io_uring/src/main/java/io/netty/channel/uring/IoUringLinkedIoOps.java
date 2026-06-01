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
 * A linked chain of {@link IoUringIoOps} submitted with one shared {@code user_data}.
 */
public final class IoUringLinkedIoOps implements IoOps {
    private final IoUringIoOps[] ops;
    private final byte[] opcodes;
    private final long[] userDatas;

    private IoUringLinkedIoOps(IoUringIoOps[] ops) {
        this.ops = ops;
        opcodes = new byte[ops.length];
        userDatas = new long[ops.length];
        for (int i = 0; i < ops.length; i++) {
            IoUringIoOps op = ops[i];
            opcodes[i] = op.opcode();
            userDatas[i] = op.userData();
        }
    }

    /**
     * Returns {@code true} if linked-chain cancellation can be supported by the kernel.
     *
     * @return {@code true} if supported, {@code false} otherwise.
     */
    public static boolean isSupported() {
        return IoUring.isAsyncCancelAllSupported();
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
        if (!isSupported()) {
            throw new UnsupportedOperationException("IoUringLinkedIoOps requires IORING_ASYNC_CANCEL_ALL support "
                    + "so the single id returned by IoRegistration.submit(...) can cancel every SQE in the "
                    + "linked chain");
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

    public int size() {
        return ops.length;
    }

    public IoUringIoOps op(int index) {
        return ops[index];
    }

    byte[] opcodes() {
        return opcodes;
    }

    long[] userDatas() {
        return userDatas;
    }

    private static byte normalizeLinkFlag(byte flags, boolean last) {
        return last ? (byte) (flags & ~Native.IOSQE_LINK) : (byte) (flags | Native.IOSQE_LINK);
    }

    private static IoUringIoOps withFlags(IoUringIoOps op, byte flags) {
        return new IoUringIoOps(op.opcode(), flags, op.ioPrio(), op.fd(), op.union1(), op.union2(), op.len(),
                op.union3(), op.userData(), op.union4(), op.personality(), op.union5(), op.union6());
    }
}
