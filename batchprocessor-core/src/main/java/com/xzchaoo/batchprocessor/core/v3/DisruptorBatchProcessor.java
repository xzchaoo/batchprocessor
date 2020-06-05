package com.xzchaoo.batchprocessor.core.v3;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.xzchaoo.batchprocessor.core.v2.SimpleThreadFactory;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-04
 */
public class DisruptorBatchProcessor<T> implements BatchProcessor<T> {

    private final Disruptor<Event<T>>         disruptor;
    private final RingBuffer<Event<T>>        ringBuffer;
    private final int                         workerCount;
    private       int                         maxBatchSize      = 1024;
    private       int                         maxConcurrency    = 10;
    private       int                         wip;
    private       ArrayDeque<List<T>>         delayed           = new ArrayDeque<>(65536);
    private       boolean                     flushOnEndOfBeach = false;
    private final ScheduledThreadPoolExecutor scheduler;

    DisruptorBatchProcessor(int workerCount, Flusher.Factory<T> factory) {
        this.workerCount = workerCount;
        disruptor = new Disruptor<>(
                Event::new,
                65536,
                new SimpleThreadFactory("test"),
                ProducerType.MULTI,
                new LiteBlockingWaitStrategy());
        Semaphore semaphore = new Semaphore(100);
        for (int i = 0; i < workerCount; i++) {
            Worker<T> worker = new Worker<>(i, maxBatchSize, factory.create(i), semaphore, true);
            disruptor.handleEventsWith(worker);
        }
        scheduler = new ScheduledThreadPoolExecutor(1,
                new SimpleThreadFactory("Foo-Scheduler"));

        // TODO flush 怎么搞?
        // disruptor.setDefaultExceptionHandler();
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
        long next = ringBuffer.next();
        Event<T> event = ringBuffer.get(next);
        event.payload = t;
    }
}
