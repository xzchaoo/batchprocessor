package com.xzchaoo.batchprocessor.core.v2;

import java.util.concurrent.ThreadFactory;

/**
 * created at 2020/5/17
 *
 * @author xzchaoo
 */
public class BatchProcessorConfig {
    private String name;
    private int workerCount = 1;
    private int concurrency = 16;
    private int flushSize = 1024;
    private int bufferSize = 65536;
    private ThreadFactory threadFactory;
    private boolean blockingOnInsufficientCapacity = false;
    private Flusher.Factory<?> flusherFactory;
    private long forceFlushIntervalMills = 1000;
    private long stopTimeoutMills = 5000;

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
}
