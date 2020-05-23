package com.xzchaoo.batchprocessor.core.v2;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
    private static final int FLUSH = 1;
    private static final int ARG2_IS_LIST = 2;
    private static final int ARG2_IS_LIST_STANDALONE = 3;
    private static final int ARG2_IS_CONTEXT = 4;
    private final Disruptor<Event<T>> disruptor;
    private final RingBuffer<Event<T>> ringBuffer;
    private final boolean blockOnNext;
    private final int flushSize;
    private final boolean copyWhenFlush;
    private final Flusher<T> flusher;
    private final Semaphore semaphore;
    private final SequenceBarrier barrier;
    private final List<T> buffer = new ArrayList<>();
    private final BatchProcessorConfig config;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> flushTaskFuture;

    Worker(int index,
           BatchProcessorConfig config,
           int flushSize,
           int bufferSize,
           ThreadFactory threadFactory,
           boolean blockOnNext,
           boolean copyWhenFlush,
           Flusher<T> flusher,
           Semaphore semaphore,
           ScheduledExecutorService scheduler) {
        this.config = config;
        this.flushSize = flushSize;
        this.blockOnNext = blockOnNext;
        this.flusher = flusher;
        this.semaphore = semaphore;
        this.copyWhenFlush = copyWhenFlush;
        this.scheduler = scheduler;
        this.disruptor = new Disruptor<>(
            Event::new,
            bufferSize,
            threadFactory,
            ProducerType.MULTI,
            new LiteBlockingWaitStrategy());
        barrier = this.disruptor.handleEventsWith(this).asSequenceBarrier();
        this.ringBuffer = disruptor.getRingBuffer();
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
        flushTaskFuture = scheduler.scheduleWithFixedDelay(this::flush,
            forceFlushIntervalMills,
            forceFlushIntervalMills,
            TimeUnit.MILLISECONDS);
    }

    void flush() {
        long cursor;
        try {
            cursor = ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            LOGGER.warn("fail to get next cursor for flush");
            return;
        }
        Event<T> event = ringBuffer.get(cursor);
        event.clear();
        event.arg_int = FLUSH;
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
        event.arg_int = ARG2_IS_LIST;
        event.arg2 = list;
        this.ringBuffer.publish(cursor);
    }

    void put(T t) {
        if (blockOnNext) {
            long cursor = this.ringBuffer.next();
            Event<T> event = this.ringBuffer.get(cursor);
            event.clear();
            event.payload = t;
            this.ringBuffer.publish(cursor);
        } else {
            if (!tryPut(t)) {
                throw new IllegalStateException("buffer is full");
            }
        }
    }

    void putAsWhole(List<T> coll, boolean needCopy) {
        long cursor = this.ringBuffer.next();
        Event<T> event = this.ringBuffer.get(cursor);
        event.clear();
        event.arg_int = ARG2_IS_LIST_STANDALONE;
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
        switch (event.arg_int) {
            case 0:
                // normal case
                buffer.add(event.payload);
                if (buffer.size() >= this.flushSize) {
                    flush0(buffer, true);
                }
                break;
            case FLUSH:
                flush0(buffer, true);
                break;
            case ARG2_IS_LIST: {
                List<T> batch = (List<T>) event.arg2;
                if (buffer.size() + batch.size() >= this.flushSize) {
                    flush0(buffer, true);
                    flush0(batch, false);
                } else {
                    this.buffer.addAll(batch);
                }
            }
            break;
            case ARG2_IS_LIST_STANDALONE: {
                flush0(buffer, true);
                List<T> batch = (List<T>) event.arg2;
                flush0(batch, false);
            }
            break;
            case ARG2_IS_CONTEXT:
                flush0((Context) event.arg2);
                break;
            default:
                break;
        }
    }

    private void flush0(List<T> batch0, boolean clear) {
        if (batch0.isEmpty()) {
            return;
        }
        List<T> batch = copyWhenFlush ? new ArrayList<>(batch0) : batch0;
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

    private void flush0(Context context) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new IllegalStateException("fail to acquire semaphore", e);
        }
        flusher.flush(context.batch, context);
    }

    public Stat.WorkerStat stat() {
        Stat.WorkerStat stat = new Stat.WorkerStat();
        stat.usedBufferSize = (int) (ringBuffer.getBufferSize() - this.ringBuffer.remainingCapacity());
        stat.totalBufferSize = ringBuffer.getBufferSize();
        return stat;
    }

    private class Context implements Flusher.Context<T> {
        private int retryCount;
        private List<T> batch;

        Context() {

        }

        @Override
        public int retryCount() {
            return retryCount;
        }

        @Override
        public int maxRetryCount() {
            return config.getMaxRetryCount();
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
            if (++retryCount > maxRetryCount()) {
                LOGGER.warn("discard retry {}", batch);
            } else {
                this.batch = batch;
                // TODO cancel this future when stop
                // try catch exception when shutdown
                scheduler.schedule(() -> {
                    putRetry(this);
                }, delayMills, TimeUnit.MILLISECONDS);
            }
        }

    }

    private void putRetry(Context context) {
        long cursor = this.ringBuffer.next();
        Event<T> event = this.ringBuffer.get(cursor);
        event.clear();
        event.arg_int = ARG2_IS_CONTEXT;
        event.arg2 = context;
        this.ringBuffer.publish(cursor);
    }
}
