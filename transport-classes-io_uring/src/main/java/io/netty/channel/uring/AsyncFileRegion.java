package io.netty.channel.uring;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.DefaultFileRegion;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.SucceededFuture;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.util.function.Function;

class AsyncFileRegion extends IoUringAsyncFile {

    private static final MethodHandle FILE_CHANEL_GETTER;

    private static final MethodHandle TRANSFERRED_SETTER;

    private static final MethodHandle VALIDATE_FILE_REGION_MH;

    static {
        try {
            Class<DefaultFileRegion> regionClass = DefaultFileRegion.class;
            Field fileField = regionClass.getDeclaredField("file");
            fileField.setAccessible(true);
            Field transferedField = regionClass.getDeclaredField("transferred");
            transferedField.setAccessible(true);

            Method method = DefaultFileRegion.class.getDeclaredMethod("validate", DefaultFileRegion.class, long.class);
            method.setAccessible(true);

            MethodHandles.Lookup lookup = MethodHandles.lookup();
            FILE_CHANEL_GETTER = lookup.unreflectGetter(fileField);
            TRANSFERRED_SETTER = lookup.unreflectSetter(transferedField);
            VALIDATE_FILE_REGION_MH = lookup.unreflect(method);
        } catch (NoSuchFieldException | IllegalAccessException| NoSuchMethodException e) {
            //should never happen
            throw new RuntimeException(e);
        }
    }

    private final DefaultFileRegion region;

    private AsyncFileRegion(DefaultFileRegion region) {
        //we may need `Flexible Constructor Bodies` to check if the file is open
        //but in jdk8, we only use factory method to create a new instance
        super(getFileChannel(region));
        this.region = region;
    }

    public static AsyncFileRegion newInstance(DefaultFileRegion defaultFileRegion) throws IOException {
        defaultFileRegion.open();
        return new AsyncFileRegion(defaultFileRegion);
    }

    private static FileChannel getFileChannel(DefaultFileRegion defaultFileRegion) {
        try {
            return (FileChannel) FILE_CHANEL_GETTER.invoke(defaultFileRegion);
        } catch (Throwable e) {
            //should never happen
            throw new RuntimeException(e);
        }
    }

    private static void setTransferred(DefaultFileRegion defaultFileRegion, long transferred) {
        try {
            TRANSFERRED_SETTER.invoke(defaultFileRegion, transferred);
        } catch (Throwable e) {
            //should never happen
            throw new RuntimeException(e);
        }
    }

    public static void validateFileRegion(DefaultFileRegion region, long transferred) throws IOException {
        try {
            VALIDATE_FILE_REGION_MH.invoke(region, transferred);
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            //should never happen
            throw new RuntimeException(e);
        }
    }

    public Future<Integer> asyncTransferTo(ChannelOutboundBuffer in, ByteBuf buf, Function<ByteBuf, Future<Integer>> target) {
        final long transferred = region.transferred();
        final long regionCount = region.count();
        if (transferred >= regionCount) {
            in.remove();
            return new SucceededFuture<>(ioEventLoop, 0);
        }

        int length = (int) Math.min(regionCount - transferred, buf.writableBytes());
        DefaultPromise<Integer> promise = new DefaultPromise<>(ioEventLoop);
        long offset = region.position() + transferred;

        buf.retain();

        // submit read operation
        read(buf, length, offset)
                .addListener(new GenericFutureListener<Future<? super AsyncFileOperatorResult>>() {
                    @Override
                    public void operationComplete(Future<? super AsyncFileOperatorResult> future) throws Exception {
                        if (future.isSuccess()) {
                            //read will modify the buf readable bytes
                            target.apply(buf)
                                    .addListener(
                                            new GenericFutureListener<Future<? super Integer>>() {
                                                @Override
                                                public void operationComplete(Future<? super Integer> future) throws Exception {
                                                    //we should release the buf after the target operation complete
                                                    buf.release();
                                                    if (future.isSuccess()) {
                                                        Integer now = (Integer) future.getNow();

                                                        if (now == 0) {
                                                           try {
                                                               validateFileRegion(region, transferred);
                                                           } catch (Throwable t) {
                                                               promise.setFailure(t);
                                                           }
                                                            return;
                                                        }

                                                        setTransferred(region, transferred + now);
                                                        in.progress(now);
                                                        if (region.transferred() >= regionCount) {
                                                            in.remove();
                                                        }
                                                        promise.setSuccess(now);
                                                    } else {
                                                        promise.setFailure(future.cause());
                                                    }
                                                }
                                            }
                                    );
                        } else {
                            buf.release();
                            promise.setFailure(future.cause());
                        }
                    }
                });

        return promise;
    }
}
