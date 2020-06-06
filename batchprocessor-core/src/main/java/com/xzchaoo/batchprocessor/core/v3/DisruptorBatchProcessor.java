package com.xzchaoo.batchprocessor.core.v3;

import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.xzchaoo.batchprocessor.core.v2.OneThreadFactory;
import com.xzchaoo.batchprocessor.core.v2.SimpleThreadFactory;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-04
 */
public class DisruptorBatchProcessor<T> implements BatchProcessor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorBatchProcessor.class);

    private static final int STATE_NEW     = 0;
    private static final int STATE_STARTED = 1;
    private static final int STATE_STOPPED = 2;

    private static final AtomicIntegerFieldUpdater<DisruptorBatchProcessor> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(DisruptorBatchProcessor.class, "state");

    private volatile int state = STATE_NEW;

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
    public int maxConcurrency() {
        return config.getMaxConcurrency();
    }

    @Override
    public int workerCount() {
        return config.getWorkerCount();
    }

    @Override
    public void start() {
        if (!STATE_UPDATER.compareAndSet(this, STATE_NEW, STATE_STARTED)) {
            return;
        }
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
            for (int i = 0; i < workerCount; i++) {
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
            for (int i = 0; i < workerCount; i++) {
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
        if (!STATE_UPDATER.compareAndSet(this, STATE_STARTED, STATE_STOPPED)) {
            return;
        }
        ScheduledThreadPoolExecutor scheduler = this.scheduler;
        this.scheduler = null;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (config.isShared()) {
            try {
                disruptor.shutdown(config.getCloseWaitTimeoutMills(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new IllegalStateException("close timeout", e);
            }
            //
        } else {
            for (Worker<T> worker : workers) {
                worker.stop(waitForAllToComplete);
            }
        }
        try {
            if (!semaphore.tryAcquire(config.getMaxConcurrency(),
                    config.getCloseWaitTimeoutMills(),
                    TimeUnit.MILLISECONDS)) {
                LOGGER.error("fail to acquire {} in {}ms",
                        config.getMaxConcurrency(),
                        config.getCloseWaitTimeoutMills());
                throw new IllegalStateException(
                        "fail to acquire " + config.getMaxConcurrency() + " in " + config.getCloseWaitTimeoutMills() + "ms");
            }
        } catch (InterruptedException e) {
            LOGGER.error("acquire semaphore interrupted", e);
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
