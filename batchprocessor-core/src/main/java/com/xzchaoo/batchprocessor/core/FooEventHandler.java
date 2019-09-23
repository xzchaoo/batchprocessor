package com.xzchaoo.batchprocessor.core;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.TimeoutHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author xzchaoo
 * @date 2019/9/6
 */
class FooEventHandler<T> implements EventHandler<Event<T>>, TimeoutHandler {
    private final int workerIndex;
    private final List<T> buffer;
    private final int bufferSize;
    private final Limiter limiter;
    private final AsyncProcessor<T> reporter;
    private final int minBatchSize;
    private final long forceFlushInterval;
    private long lastUpdateTime;

    FooEventHandler(int workerIndex,
                    int batchSize,
                    int minBatchSize,
                    long forceFlushInterval,
                    Limiter limiter,
                    AsyncProcessor<T> reporter) {
        this.workerIndex = workerIndex;
        this.bufferSize = batchSize;
        this.buffer = new ArrayList<>(batchSize);
        this.minBatchSize = Math.min(batchSize, minBatchSize);
        this.forceFlushInterval = forceFlushInterval;
        this.limiter = Objects.requireNonNull(limiter);
        this.reporter = Objects.requireNonNull(reporter);
    }

    @Override
    public void onEvent(Event<T> event, long sequence, boolean endOfBatch) throws Exception {
        // 只处理自己的
        if (event.workerIndex != workerIndex) {
            return;
        }

        // 处理flush
        if (event.flush) {
            flush();
            return;
        }

        buffer.add(event.t);

        // 检查是否需要flush
        boolean needFlush = buffer.size() >= bufferSize
            || endOfBatch &&
            (buffer.size() >= minBatchSize || System.currentTimeMillis() - lastUpdateTime >= forceFlushInterval);

        if (needFlush) {
            flush();
        }
    }

    private void flush() {
        if (buffer.isEmpty()) {
            lastUpdateTime = System.currentTimeMillis();
            return;
        }
        try {
            flush1();
        } finally {
            lastUpdateTime = System.currentTimeMillis();
            buffer.clear();
        }
    }

    private void flush1() {
        try {
            limiter.acquireConcurrency();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        ListenableFuture<?> future;
        try {
            limiter.acquireRate(buffer.size());
            future = reporter.asyncProcess(buffer);
        } catch (Throwable e) {
            limiter.releaseConcurrency();
            throw new RuntimeException(e);
        }

        future.addListener(limiter::releaseConcurrency, MoreExecutors.directExecutor());
    }

    @Override
    public void onTimeout(long sequence) throws Exception {
        flush();
    }
}
