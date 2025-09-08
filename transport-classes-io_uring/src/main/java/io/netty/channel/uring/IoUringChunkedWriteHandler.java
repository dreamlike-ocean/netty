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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.IoEventLoop;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.io.File;
import java.io.IOException;

public class IoUringChunkedWriteHandler extends ChunkedWriteHandler {
    private volatile IoEventLoop ioEventLoop;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
        this.ioEventLoop = ((IoEventLoop) ctx.channel().eventLoop());
    }

    public ChunkedIoUringFile newChunkedIoUringFile(File file) throws IOException {
        ChunkedIoUringFile chunkedIoUringFile = new ChunkedIoUringFile(file, this);
        chunkedIoUringFile.register(ioEventLoop);
        return chunkedIoUringFile;
    }

    //todo other ctor
}
