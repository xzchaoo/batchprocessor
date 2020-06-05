package com.xzchaoo.batchprocessor.core.v3;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.xzchaoo.batchprocessor.core.v2.SimpleThreadFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-04
 */
public class DisruptorBatchProcessor<T> implements BatchProcessor<T> {

    private final Disruptor<Event<T>> disruptor;
    private final RingBuffer<Event<T>> ringBuffer;
    private final int workerCount;
    private final int maxBatchSize;
    private final int maxConcurrency;
    private int wip;
    private ArrayDeque<List<T>> delayed = new ArrayDeque<>(65536);
    private boolean flushOnEndOfBeach = false;
    private final ScheduledThreadPoolExecutor scheduler;

    public DisruptorBatchProcessor(int workerCount, int maxBatchSize, int maxConcurrency, Flusher.Factory<T> factory) {
        this.workerCount = workerCount;
        this.maxBatchSize = maxBatchSize;
        this.maxConcurrency = maxConcurrency;
        disruptor = new Disruptor<>(
            Event::new,
            65536,
            new SimpleThreadFactory("test"),
            ProducerType.MULTI,
            new LiteBlockingWaitStrategy());
        Semaphore semaphore = new Semaphore(maxConcurrency);
        for (int i = 0; i < workerCount; i++) {
            Worker<T> worker = new Worker<>(i, maxBatchSize, factory.create(i), semaphore, true);
            disruptor.handleEventsWith(worker);
        }
        scheduler = new ScheduledThreadPoolExecutor(1,
            new SimpleThreadFactory("Foo-Scheduler"));
        ringBuffer = disruptor.getRingBuffer();
    }

    @Override
    public int workerCount() {
        return workerCount;
    }

    @Override
    public void start() {
        disruptor.start();
        scheduler.scheduleWithFixedDelay(this::flush, 1, 1, TimeUnit.SECONDS);
    }

    private void flush() {
        long cursor;
        try {
            cursor = ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            return;
        }
        Event<T> event = ringBuffer.get(cursor);
        event.action = Worker.ACTION_FLUSH_ALL;
    }

    @Override
    public void stop() {
        disruptor.shutdown();
    }

    @Override
    public boolean tryPut(T t) {
        return false;
    }

    @Override
    public void put(T t) {
        if (t == null) {
            throw new NullPointerException();
        }
        long cursor = ringBuffer.next();
        Event<T> event = ringBuffer.get(cursor);
        event.index = 0;
        event.action = Worker.ACTION_ADD;
        event.payload = t;
        ringBuffer.publish(cursor);
    }
}
