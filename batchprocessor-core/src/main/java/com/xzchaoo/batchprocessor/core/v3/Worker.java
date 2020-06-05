package com.xzchaoo.batchprocessor.core.v3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventHandler;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-05
 */
class Worker<T> implements EventHandler<Event<T>> {
    private static final Logger LOGGER           = LoggerFactory.getLogger(Worker.class);
    static final         int    ACTION_ADD       = 0;
    static final         int    ACTION_FLUSH     = 1;
    static final         int    ACTION_FLUSH_ALL = 2;

    private final int        index;
    private final Flusher<T> flusher;
    private final int        maxBatch;
    private final List<T>    buffer;
    private final Semaphore  semaphore;
    private final boolean    flushOnEndOfBatch;
    private final int        maxRetryCount = 3;

    Worker(int index, int maxBatch, Flusher<T> flusher, Semaphore semaphore, boolean flushOnEndOfBatch) {
        this.index = index;
        this.maxBatch = maxBatch;
        this.flusher = flusher;
        this.buffer = new ArrayList<>(maxBatch);
        this.semaphore = semaphore;
        this.flushOnEndOfBatch = flushOnEndOfBatch;
    }

    @Override
    public void onEvent(Event<T> event, long sequence, boolean endOfBatch) throws Exception {
        if (event.index != index) {
            return;
        }

        switch (event.action) {
            case ACTION_ADD:
                buffer.add(event.payload);
                if (buffer.size() == maxBatch) {
                    flush(buffer, true);
                }
                event.clear();
                break;
            case ACTION_FLUSH:
                flush(buffer, true);
                event.clear();
                break;
            case ACTION_FLUSH_ALL:
                break;
            default:
                break;
        }

        if (flushOnEndOfBatch && endOfBatch) {
            flush(buffer, true);
        }

    }

    private void flush(List<T> batch, boolean clear) {
        if (batch.isEmpty()) {
            return;
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            LOGGER.error("thread interrupted while acquiring semaphore", e);
            Thread.currentThread().interrupt();
            stop();
            return;
        }

        try {
            List<T> copy = new ArrayList<>(batch);
            flusher.flush(copy, new Context());
        } finally {
            if (clear) {
                batch.clear();
            }
        }
    }

    void start() {}

    void stop() {
    }

    class Context implements Flusher.Context {
        private int retryCount;

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
                return true;
            }
            return false;
        }
    }
}
