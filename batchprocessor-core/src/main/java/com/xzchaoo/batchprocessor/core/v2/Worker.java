package com.xzchaoo.batchprocessor.core.v2;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * created at 2020/5/17
 *
 * @author xzchaoo
 */
@SuppressWarnings("unchecked")
class Worker<T> implements EventHandler<Event<T>> {
    private static final Object FLUSH = new Object();
    private static final Object ARG2_IS_LIST = new Object();
    private static final Object ARG2_IS_LIST_STANDALONE = new Object();
    private final Disruptor<Event<T>> disruptor;
    private final RingBuffer<Event<T>> ringBuffer;
    private final boolean blockOnNext;
    private final int flushSize;
    private final boolean copyFlushBuffer = false;
    private final Flusher<T> flusher;
    private final Semaphore semaphore;
    private final SequenceBarrier barrier;
    private final List<T> buffer = new ArrayList<>();
    private ScheduledFuture<?> flushTaskFuture;

    Worker(int index, int flushSize, int bufferSize, ThreadFactory threadFactory, boolean blockOnNext,
           Flusher<T> flusher, Semaphore semaphore) {
        this.flushSize = flushSize;
        this.blockOnNext = blockOnNext;
        this.flusher = flusher;
        this.semaphore = semaphore;
        this.disruptor = new Disruptor<>(
            Event::new,
            bufferSize,
            threadFactory,
            ProducerType.MULTI,
            new LiteBlockingWaitStrategy());
        barrier = this.disruptor.handleEventsWith(this).asSequenceBarrier();
        this.ringBuffer = disruptor.getRingBuffer();
        // TODO 怎么控制定期强制flush
    }

    void start(long forceFlushIntervalMills) {
        this.disruptor.start();
        flush();
        try {
            barrier.waitFor(0);
        } catch (Exception e) {
            throw new IllegalStateException("fail to init disruptor");
        }
        // TODO cancel when stop
        flushTaskFuture = DisruptorBatchProcessor.SCHEDULER.scheduleWithFixedDelay(this::flush,
            forceFlushIntervalMills,
            forceFlushIntervalMills,
            TimeUnit.MILLISECONDS);
    }

    void flush() {
        long cursor;
        try {
            cursor = ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            return;
        }
        Event<T> event = ringBuffer.get(cursor);
        event.clear();
        event.arg1 = FLUSH;
        ringBuffer.publish(cursor);
    }

    void stop(long stopTimeoutMills) {
        ScheduledFuture<?> flushTaskFuture = this.flushTaskFuture;
        this.flushTaskFuture = null;
        if (flushTaskFuture != null) {
            flushTaskFuture.cancel(true);
        }
        // 等待全部完成
        try {
            disruptor.shutdown(stopTimeoutMills, TimeUnit.MILLISECONDS);
            // 最后可能会残留一些在buffer里 可以清掉
            flush0(buffer, true);
        } catch (TimeoutException e) {
            throw new RuntimeException("shutdown disruptor error", e);
        }
    }

    boolean tryPut(T t) {
        long cursor;
        try {
            cursor = this.ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            return false;
        }
        Event<T> event = this.ringBuffer.get(cursor);
        event.clear();
        event.payload = t;
        this.ringBuffer.publish(cursor);
        return true;
    }

    void put(List<T> list) {
        long cursor = this.ringBuffer.next();
        Event<T> event = this.ringBuffer.get(cursor);
        event.clear();
        event.arg1 = ARG2_IS_LIST;
        event.arg2 = list;
        this.ringBuffer.publish(cursor);
    }

    void put(T t) {
        if (blockOnNext) {
            long cursor = this.ringBuffer.next();
            publish(t, cursor);
        } else {
            if (!tryPut(t)) {
                throw new IllegalStateException("buffer is full");
            }
        }
    }

    private void publish(T t, long cursor) {
        Event<T> event = this.ringBuffer.get(cursor);
        event.clear();
        event.payload = t;
        this.ringBuffer.publish(cursor);
    }

    void putAsWhole(List<T> coll, boolean needCopy) {
        long cursor = this.ringBuffer.next();
        Event<T> event = this.ringBuffer.get(cursor);
        event.clear();
        event.arg1 = ARG2_IS_LIST_STANDALONE;
        event.arg2 = needCopy ? new ArrayList<>(coll) : coll;
        this.ringBuffer.publish(cursor);
    }

    boolean tryPut(List<T> list) {
        int n = list.size();
        long hi;
        try {
            hi = this.ringBuffer.tryNext(n);
        } catch (InsufficientCapacityException e) {
            return false;
        }
        long lo = hi - n + 1;
        long cursor = lo;
        for (T t : list) {
            Event<T> event = this.ringBuffer.get(cursor);
            event.clear();
            event.payload = t;
            ++cursor;
        }
        this.ringBuffer.publish(lo, hi);
        return true;
    }

    @Override
    public void onEvent(Event<T> event, long sequence, boolean endOfBatch) throws Exception {
        Object arg1 = event.arg1;
        if (arg1 == null) {
            // normal case
            buffer.add(event.payload);
            if (buffer.size() >= this.flushSize) {
                flush0(buffer, true);
            }
        } else if (arg1 == FLUSH) {
            flush0(buffer, true);
        } else if (arg1 == ARG2_IS_LIST) {
            List<T> batch = (List<T>) event.arg2;
            if (buffer.size() + batch.size() >= this.flushSize) {
                flush0(buffer, true);
                flush0(batch, false);
            } else {
                this.buffer.addAll(batch);
            }
        } else if (arg1 == ARG2_IS_LIST_STANDALONE) {
            flush0(buffer, true);
            List<T> batch = (List<T>) event.arg2;
            flush0(batch, false);
        }
    }

    private void flush0(List<T> batch0, boolean clear) {
        if (batch0.isEmpty()) {
            return;
        }
        List<T> batch = copyFlushBuffer ? new ArrayList<>(batch0) : batch0;
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new IllegalStateException("fail to acquire semaphore", e);
        }
        try {
            flusher.flush(batch, new Context());
        } finally {
            if (clear) {
                batch0.clear();
            }
        }
    }

    private class Context implements Flusher.Context<T> {
        Context() {

        }

        @Override
        public void complete() {
            semaphore.release();
        }

        /**
         * TODO 重试次数限制 否则会无限循环了
         *
         * @param delayMills
         * @param batch
         */
        @Override
        public void retry(long delayMills, List<T> batch) {
            semaphore.release();
            DisruptorBatchProcessor.SCHEDULER.schedule(() -> putAsWhole(batch, false), delayMills,
                TimeUnit.MILLISECONDS);
        }
    }
}
