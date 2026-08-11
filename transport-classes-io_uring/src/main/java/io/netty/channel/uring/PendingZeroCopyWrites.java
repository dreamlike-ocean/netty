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

import io.netty.buffer.ByteBuf;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.LongObjectMap;

import java.util.ArrayList;
import java.util.List;

final class PendingZeroCopyWrites {
    static final int MAX_UNSIGNED_SHORT = 0xFFFF;
    private static final long USER_DATA_STRIDE = MAX_UNSIGNED_SHORT + 1L;

    private final LongObjectMap<List<ByteBuf>> pendingWrites = new LongObjectHashMap<>(4);

    long nextLongUserData(short candidate) {
        long userData = candidate;
        do {
            userData += USER_DATA_STRIDE;
        } while (pendingWrites.containsKey(userData));
        return userData;
    }

    List<ByteBuf> register(long userData) {
        List<ByteBuf> buffers = new ArrayList<>();
        assert !pendingWrites.containsKey(userData);
        pendingWrites.put(userData, buffers);
        return buffers;
    }

    void release(long userData) {
        List<ByteBuf> buffers = pendingWrites.remove(userData);
        if (buffers == null) {
            return;
        }
        for (ByteBuf buffer : buffers) {
            buffer.release();
        }
    }

    boolean contains(long userData) {
        return pendingWrites.containsKey(userData);
    }
}
