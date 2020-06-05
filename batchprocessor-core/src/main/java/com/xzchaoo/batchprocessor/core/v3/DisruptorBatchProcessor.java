package com.xzchaoo.batchprocessor.core.v3;

import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.xzchaoo.batchprocessor.core.v2.OneThreadFactory;
import com.xzchaoo.batchprocessor.core.v2.SimpleThreadFactory;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-04
 */
public class DisruptorBatchProcessor<T> implements BatchProcessor<T> {

    // shared begin
    private Disruptor<Event<T>> disruptor;
    // shared end

    private final BatchProcessorConfig config;
    private final int                  workerCount;

    private       ScheduledThreadPoolExecutor scheduler;
    private final Semaphore                   semaphore;
    private final Flusher.Factory<T>          factory;

    @SuppressWarnings("unchecked")
    private Worker<T>[] workers;
    private Worker      worker;

    public DisruptorBatchProcessor(BatchProcessorConfig config, Flusher.Factory<T> factory) {
        this.workerCount = config.getWorkerCount();
        this.config = config;
        this.semaphore = new Semaphore(config.getMaxConcurrency());
        this.factory = factory;
    }

    @Override
    public int workerCount() {
        return config.getWorkerCount();
    }

    @Override
    public void start() {
        this.scheduler = new ScheduledThreadPoolExecutor(1, new OneThreadFactory(config.getName() + "-Scheduler"));
        Duration interval = config.getFlushInterval();
        workers = new Worker[config.getWorkerCount()];
        if (config.isShared()) {
            this.disruptor = new Disruptor<>(
                    Event::new,
                    config.getRingBufferSize(),
                    new SimpleThreadFactory(config.getName()),
                    ProducerType.MULTI,
                    new LiteBlockingWaitStrategy());
            RingBuffer<Event<T>> ringBuffer = disruptor.getRingBuffer();
            for (int i = 0; i < config.getWorkerCount(); i++) {
                SharedWorker<T> worker = new SharedWorker<>(i,
                        config,
                        factory.create(i),
                        scheduler,
                        semaphore,
                        ringBuffer);
                workers[i] = worker;
                if (i == 0) {
                    this.worker = worker;
                }
                this.disruptor.handleEventsWith(worker);
            }
            disruptor.start();
            scheduler.scheduleWithFixedDelay(() -> {
                long cursor;
                try {
                    cursor = ringBuffer.tryNext();
                } catch (InsufficientCapacityException e) {
                    return;
                }
                Event<T> event = ringBuffer.get(cursor);
                event.action = Worker.ACTION_FLUSH;
            }, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        } else {
            for (int i = 0; i < config.getWorkerCount(); i++) {
                StandaloneWorker<T> worker = new StandaloneWorker<>(i, config, scheduler, semaphore, factory.create(i));
                workers[i] = worker;
                worker.start();
                if (i == 0) {
                    this.worker = worker;
                }
            }
            scheduler.scheduleWithFixedDelay(() -> {
                for (Worker<T> worker : workers) {
                    worker.flush();
                }
            }, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void stop(boolean waitForAllToComplete) {
        ScheduledThreadPoolExecutor scheduler = this.scheduler;
        this.scheduler = null;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (config.isShared()) {
            disruptor.shutdown();
        } else {
            for (Worker<T> worker : workers) {
                worker.stop(waitForAllToComplete);
            }
        }
    }

    @Override
    public boolean tryPut(T t) {
        return nextWorker().tryPut(t);
    }

    @Override
    public void put(T t) {
        nextWorker().put(t);
    }

    private int workerIndex;

    private Worker<T> nextWorker() {
        if (workerCount == 1) {
            return worker;
        }
        int index = (this.workerIndex++ & 0X7FFF_FFFF) % (workerCount);
        return workers[index];
    }
}
