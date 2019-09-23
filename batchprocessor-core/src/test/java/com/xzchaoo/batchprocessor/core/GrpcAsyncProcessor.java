package com.xzchaoo.batchprocessor.core;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xzchaoo
 * @date 2019/9/7
 */
public class GrpcAsyncProcessor implements AsyncProcessor<Foo> {
    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor();

    private AtomicInteger finished = new AtomicInteger();

    @Override
    public ListenableFuture<Object> asyncProcess(List<Foo> batch) {
        int size = batch.size();
        System.out.println(" 通过grpc上报数据 " + size);
        SettableFuture<Object> sf = SettableFuture.create();
        SCHEDULER.schedule(() -> {
            System.out.println("请求完成 " + finished.addAndGet(size));
            sf.set("");
        }, 1, TimeUnit.SECONDS);
        return sf;
    }
}
