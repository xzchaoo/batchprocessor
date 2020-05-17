package com.xzchaoo.batchprocessor.core.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * created at 2020/5/17
 *
 * @author xzchaoo
 */
@SuppressWarnings("unchecked")
public class DisruptorBatchProcessor<T> implements BatchProcessor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorBatchProcessor.class);

    private static final int STATE_NEW = 0;
    private static final int STATE_STARTED = 1;
    private static final int STATE_STOPPED = 2;
    private static final AtomicIntegerFieldUpdater UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(DisruptorBatchProcessor.class, "state");
    static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor();
    private final BatchProcessorConfig config;
    private final Worker<T>[] workers;
    private final int workerCount;
    private volatile int state = STATE_NEW;
    private final Semaphore semaphore;

    public DisruptorBatchProcessor(BatchProcessorConfig config) {
        this.config = Objects.requireNonNull(config);
        int workerCount = config.getWorkerCount();
        if (workerCount < 0) {
            throw new IllegalArgumentException();
        }
        this.workerCount = workerCount;
        workers = new Worker[workerCount];
        semaphore = new Semaphore(config.getConcurrency());
        ThreadFactory threadFactory = config.getThreadFactory();
        if (threadFactory == null) {
            threadFactory = new SimpleThreadFactory(config.getName());
            // config
        }
        for (int i = 0; i < workerCount; i++) {
            // waitStrategy 不能共享
            workers[i] = new Worker<>(i,
                config.getFlushSize(),
                config.getBufferSize(),
                threadFactory,
                config.isBlockingOnInsufficientCapacity(),
                (Flusher<T>) config.getFlusherFactory().create(i),
                semaphore);
        }
    }

    private final AtomicInteger workerIndex = new AtomicInteger();

    private Worker<T> nextWorker() {
        if (workerCount == 1) {
            return workers[0];
        }
        int i;
        while (true) {
            i = workerIndex.get();
            int n = (i + 1 == workerCount) ? 0 : (i + 1);
            if (workerIndex.compareAndSet(i, n)) {
                break;
            }
        }
        return workers[i];
    }

    @Override
    public boolean tryPut(T t) {
        return nextWorker().tryPut(t);
    }

    @Override
    public void put(T t) {
        nextWorker().put(t);
    }

    @Override
    public boolean tryPut(List<T> list) {
        if (list.isEmpty()) {
            return true;
        }
        // 不可分割
        return nextWorker().tryPut(list);
    }

    @Override
    public void put(List<T> list) {
        // 批量put方法导致数量统计错误

        // 可以分割到多个worker里去
        int bufferSize = this.config.getBufferSize();
        if (list.size() <= bufferSize) {
            nextWorker().put(new ArrayList<>(list));
            return;
        }
        List<T> buffer = new ArrayList<>(bufferSize);
        for (T t : list) {
            buffer.add(t);
            if (buffer.size() == bufferSize) {
                nextWorker().putAsWhole(buffer, true);
                buffer.clear();
            }
        }
        if (buffer.size() > 0) {
            nextWorker().putAsWhole(buffer, false);
        }
    }

    @Override
    public void flush() {
        for (Worker<T> worker : workers) {
            worker.flush();
        }
    }

    @Override
    public void start() {
        if (UPDATER.compareAndSet(this, STATE_NEW, STATE_STARTED)) {
            long forceFlushIntervalMills = config.getForceFlushIntervalMills();
            for (Worker<T> worker : workers) {
                worker.start(forceFlushIntervalMills);
            }
        } else {
            int state = this.state;
            if (state == STATE_STARTED) {
                LOGGER.info("This BatchProcessor has been started");
            } else {
                LOGGER.info("This BatchProcessor has been stopped");
            }
        }
    }

    @Override
    public void stop() {
        if (UPDATER.compareAndSet(this, STATE_STARTED, STATE_STOPPED)) {
            for (Worker<T> worker : workers) {
                worker.stop(config.getStopTimeoutMills());
            }
            try {
                // 保证没有 in flights 的请求
                semaphore.tryAcquire(this.config.getConcurrency(), config.getStopTimeoutMills(), TimeUnit.MILLISECONDS);
                semaphore.release(this.config.getConcurrency());
            } catch (InterruptedException e) {
                throw new IllegalStateException("fail to wait", e);
            }
        } else {
            int state = this.state;
            if (state == STATE_NEW) {
                LOGGER.info("This BatchProcessor has not been started");
            } else {
                LOGGER.info("This BatchProcessor has been stopped");
            }
        }
    }
}
