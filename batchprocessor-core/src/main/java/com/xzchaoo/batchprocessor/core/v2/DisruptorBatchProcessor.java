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
    private static final AtomicIntegerFieldUpdater STATE_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(DisruptorBatchProcessor.class, "state");
    private static final AtomicIntegerFieldUpdater WORKER_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(DisruptorBatchProcessor.class, "workerIndex");

    private final BatchProcessorConfig config;
    private final Worker<T>[] workers;
    private final Worker<T> worker0;
    private final int workerCountMinus1;
    private volatile int state = STATE_NEW;
    private final Semaphore semaphore;
    private volatile int workerIndex;
    private final ScheduledExecutorService scheduler;

    public DisruptorBatchProcessor(BatchProcessorConfig config) {
        this.config = Objects.requireNonNull(config);
        Objects.requireNonNull(config.getName());
        Objects.requireNonNull(config.getFlusherFactory());
        int workerCount = config.getWorkerCount();
        if (workerCount < 0) {
            throw new IllegalArgumentException();
        }
        this.workerCountMinus1 = workerCount - 1;
        this.workers = new Worker[workerCount];
        this.semaphore = new Semaphore(config.getConcurrency());
        ThreadFactory threadFactory = config.getThreadFactory();
        if (threadFactory == null) {
            threadFactory = new SimpleThreadFactory(config.getName());
            // config
        }
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new SimpleThreadFactory(
            config.getName() + "-scheduler"));
        for (int i = 0; i < workerCount; i++) {
            // waitStrategy 不能共享
            this.workers[i] = new Worker<>(i,
                config,
                config.getFlushSize(),
                config.getBufferSize(),
                threadFactory,
                config.isBlockingOnInsufficientCapacity(),
                config.isCopyWhenFlush(),
                (Flusher<T>) config.getFlusherFactory().create(i),
                this.semaphore,
                this.scheduler);
        }
        worker0 = workerCount == 1 ? workers[0] : null;
    }

    private Worker<T> nextWorker() {
        if (worker0 != null) {
            return worker0;
        }
        int i;
        while (true) {
            i = workerIndex;
            int n = (i == workerCountMinus1) ? 0 : (i + 1);
            if (WORKER_UPDATER.compareAndSet(this, i, n)) {
                return workers[i];
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
        if (list.isEmpty()) {
            return;
        }
        // 批量put方法导致数量统计错误 bufferSize 参数会失去意义
        // 有一种办法是用一些NULL的event占据位置

        // 可以分割到多个worker里去
        int flushSize = this.config.getFlushSize();
        if (list.size() <= flushSize) {
            nextWorker().put(new ArrayList<>(list));
            return;
        }
        List<T> batch = new ArrayList<>(flushSize);
        for (T t : list) {
            batch.add(t);
            if (batch.size() == flushSize) {
                nextWorker().putAsWhole(batch, true);
                batch.clear();
            }
        }
        if (batch.size() > 0) {
            nextWorker().putAsWhole(batch, false);
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
        if (STATE_UPDATER.compareAndSet(this, STATE_NEW, STATE_STARTED)) {
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
        if (STATE_UPDATER.compareAndSet(this, STATE_STARTED, STATE_STOPPED)) {
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
            scheduler.shutdownNow();
        } else {
            int state = this.state;
            if (state == STATE_NEW) {
                LOGGER.info("This BatchProcessor has not been started");
            } else {
                LOGGER.info("This BatchProcessor has been stopped");
            }
        }
    }

    @Override
    public Stat stat() {
        Stat stat = new Stat();
        switch (this.state) {
            case STATE_NEW:
                stat.state = "new";
                break;
            case STATE_STARTED:
                stat.state = "started";
                break;
            case STATE_STOPPED:
                stat.state = "stopped";
                break;
            default:
                break;
        }

        for (Worker<T> worker : workers) {
            Stat.WorkerStat workerStat = worker.stat();
            stat.usedBufferSize += workerStat.usedBufferSize;
            stat.totalBufferSize += workerStat.totalBufferSize;
            stat.getWorkers().add(workerStat);
        }
        stat.semaphoreStat.availablePermits = this.semaphore.availablePermits();
        stat.semaphoreStat.maxPermits = this.config.getConcurrency();
        stat.semaphoreStat.queuedSize = this.semaphore.getQueueLength();
        return stat;
    }
}
