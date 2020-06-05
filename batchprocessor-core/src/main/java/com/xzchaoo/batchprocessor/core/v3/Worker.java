package com.xzchaoo.batchprocessor.core.v3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-05
 */
abstract class Worker<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    static final int ACTION_ADD   = 0;
    static final int ACTION_FLUSH = 1;
    static final int ACTION_RETRY = 2;

    final         int                      index;
    final         ScheduledExecutorService scheduler;
    final         BatchProcessorConfig     config;
    final         int                      maxBatchSize;
    final         List<T>                  buffer;
    final         Semaphore                semaphore;
    private final int                      maxRetryCount;
    private final boolean                  flushOnEndOfBatch;
    private final Flusher<T>               flusher;

    RingBuffer<Event<T>> ringBuffer;

    Worker(int index, BatchProcessorConfig config, ScheduledExecutorService scheduler, Semaphore semaphore, Flusher<T> flusher) {
        this.index = index;
        this.config = config;
        this.scheduler = scheduler;
        this.maxBatchSize = config.getMaxBatchSize();
        this.buffer = new ArrayList<>(maxBatchSize);
        this.semaphore = semaphore;
        this.maxRetryCount = config.getMaxRetryCount();
        this.flushOnEndOfBatch = config.isFlushOnEndOfBatch();
        this.flusher = flusher;
    }

    void start() {
        if (this.ringBuffer == null) {
            throw new IllegalStateException("ringBuffer is null");
        }
    }

    void stop(boolean waitForAllToComplete) {
    }

    boolean tryPut(T t) {
        long cursor;
        try {
            cursor = ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            return false;
        }
        Event<T> event = ringBuffer.get(cursor);
        event.index = index;
        event.action = ACTION_ADD;
        event.payload = t;
        ringBuffer.publish(cursor);
        return false;
    }

    void put(T t) {
        long cursor = ringBuffer.next();
        Event<T> event = ringBuffer.get(cursor);
        event.index = index;
        event.action = ACTION_ADD;
        event.payload = t;
        ringBuffer.publish(cursor);
    }

    @Deprecated
    void flush() {
        long cursor;
        try {
            cursor = ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            return;
        }
        Event<T> event = ringBuffer.get(cursor);
        event.index = index;
        event.action = ACTION_FLUSH;
        ringBuffer.publish(cursor);
    }

    public void onEvent(Event<T> event, long sequence, boolean endOfBatch) throws Exception {
        switch (event.action) {
            case ACTION_ADD:
                if (event.index != index) {
                    return;
                }
                buffer.add(event.payload);
                if (buffer.size() == maxBatchSize) {
                    flush(buffer, true);
                }
                event.clear();
                break;
            case ACTION_FLUSH:
                if (event.index != index) {
                    return;
                }
                flush(buffer, true);
                // 此处不能清理
                break;
            case ACTION_RETRY:
                if (event.index != index) {
                    return;
                }
                flush((SharedWorker.Context) event.arg1);
                event.clear();
                break;
            default:
                break;
        }

        if (flushOnEndOfBatch && endOfBatch) {
            flush(buffer, true);
        }
    }

    void flush(Context ctx) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            LOGGER.error("thread interrupted while acquiring semaphore", e);
            Thread.currentThread().interrupt();
            return;
        }
        flusher.flush(ctx.batch, ctx);
    }

    void flush(List<T> batch, boolean clear) {
        if (batch.isEmpty()) {
            return;
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            LOGGER.error("thread interrupted while acquiring semaphore", e);
            Thread.currentThread().interrupt();
            return;
        }

        try {
            List<T> copy = new ArrayList<>(batch);
            flusher.flush(copy, new Context(copy));
        } finally {
            if (clear) {
                batch.clear();
            }
        }
    }

    class Context implements Flusher.Context {
        final List<T> batch;
        int retryCount;

        Context(List<T> batch) {
            this.batch = batch;
        }

        @Override
        public void complete() {
            semaphore.release();
        }

        @Override
        public int retryCount() {
            return retryCount;
        }

        @Override
        public int maxRetryCount() {
            return maxRetryCount;
        }

        @Override
        public boolean retry(long delayMills) {
            semaphore.release();
            if (retryCount < maxRetryCount) {
                // schedule
                ++retryCount;
                scheduler.schedule(() -> {
                    long cursor;
                    try {
                        cursor = ringBuffer.tryNext();
                    } catch (InsufficientCapacityException e) {
                        LOGGER.error("ringBuffer is full when retry", e);
                        return;
                    }
                    Event<T> event = ringBuffer.get(cursor);
                    event.index = index;
                    event.action = ACTION_RETRY;
                    event.arg1 = this;
                    ringBuffer.publish(cursor);
                }, delayMills, TimeUnit.MILLISECONDS);
                return true;
            }
            return false;
        }
    }

}
