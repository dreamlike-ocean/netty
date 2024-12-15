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

import io.netty.channel.unix.FileDescriptor;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Its a simple pipeFd pool implement, which is only used in single-threaded scenarios.
 */
class PipeFdPool {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PipeFdPool.class);

    private final int maxCapacity;

    private final Queue<FileDescriptor[]> pipeFdPool;

    PipeFdPool(int maxCapacity) {
        this.maxCapacity = maxCapacity;
        pipeFdPool = new ArrayDeque<>(maxCapacity);
    }

    public FileDescriptor[] acquire() throws IOException {
        FileDescriptor[] fds = pipeFdPool.poll();
        //fastPath
        //We assume that obtaining the pipefd is a low-frequency operation.
        if (fds != null) {
          return fds;
        }

        return FileDescriptor.pipe();
    }

    public void release(FileDescriptor[] fds) {
        assert fds != null;
        if (pipeFdPool.size() < maxCapacity) {
            pipeFdPool.offer(fds);
        } else {
            safeClose(fds[0]);
            safeClose(fds[1]);
        }
    }

    public void destroy() {
        for (FileDescriptor[] fds : pipeFdPool) {
            safeClose(fds[0]);
            safeClose(fds[1]);
        }
    }

    private static void safeClose(FileDescriptor fd) {
        try {
            fd.close();
        } catch (IOException e) {
            logger.warn("Failed to close a pipeFd.", e);
        }
    }
}
