package com.xzchaoo.batchprocessor.core.v2;

import java.util.concurrent.ThreadFactory;

/**
 * created at 2020/5/17
 *
 * @author xzchaoo
 */
public class BatchProcessorConfig {
    /**
     * [required] name of instance
     */
    private String name = "default";
    /**
     * [required] flusher factory
     */
    private Flusher.Factory<?> flusherFactory;
    /**
     * backend worker thread count, often 1 worker is enough, otherwise
     */
    private int workerCount = 1;
    /**
     * max concurrency in-flights requests, shared by all workers
     */
    private int concurrency = 16;
    /**
     * how many items should bu flush in a batch
     */
    private int flushSize = 1024;
    /**
     * buffer size every work has
     */
    private int bufferSize = 65536;
    /**
     * thread factory for workers, defaults to "${name}-%d"
     */
    private ThreadFactory threadFactory;
    /**
     * blocks on insufficient capacity or else throws an exception
     */
    private boolean blockingOnInsufficientCapacity = false;
    /**
     * interval for a force flush
     */
    private long forceFlushIntervalMills = 1000;
    /**
     * stop(shutdown) wait timeout
     */
    private long stopTimeoutMills = 5000;
    /**
     * Copy buffer when flush. When false, same internal buffer will be fed to flusher, this buffer will be clear after
     * flush method call.  Flusher should not hold reference to buffer instance after flush method call.
     */
    private boolean copyWhenFlush = true;
    /**
     * Max retry count, any retry whose count exceed this value will be discard and log a warn.
     */
    private int maxRetryCount = 1;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int getFlushSize() {
        return flushSize;
    }

    public void setFlushSize(int flushSize) {
        this.flushSize = flushSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }

    public boolean isBlockingOnInsufficientCapacity() {
        return blockingOnInsufficientCapacity;
    }

    public void setBlockingOnInsufficientCapacity(boolean blockingOnInsufficientCapacity) {
        this.blockingOnInsufficientCapacity = blockingOnInsufficientCapacity;
    }

    public Flusher.Factory<?> getFlusherFactory() {
        return flusherFactory;
    }

    public void setFlusherFactory(Flusher.Factory<?> flusherFactory) {
        this.flusherFactory = flusherFactory;
    }

    public long getForceFlushIntervalMills() {
        return forceFlushIntervalMills;
    }

    public void setForceFlushIntervalMills(long forceFlushIntervalMills) {
        this.forceFlushIntervalMills = forceFlushIntervalMills;
    }

    public long getStopTimeoutMills() {
        return stopTimeoutMills;
    }

    public void setStopTimeoutMills(long stopTimeoutMills) {
        this.stopTimeoutMills = stopTimeoutMills;
    }

    public boolean isCopyWhenFlush() {
        return copyWhenFlush;
    }

    public void setCopyWhenFlush(boolean copyWhenFlush) {
        this.copyWhenFlush = copyWhenFlush;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }
}
