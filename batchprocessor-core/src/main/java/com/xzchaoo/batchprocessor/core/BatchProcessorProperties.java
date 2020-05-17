package com.xzchaoo.batchprocessor.core;

import java.util.concurrent.ThreadFactory;

/**
 * @author xzchaoo
 */
public class BatchProcessorProperties {
    /**
     * disruptor 队列大小, 必须是2^n
     */
    private int queueSize = 65536;
    /**
     * 每多少个元素就flush一次
     */
    private int batchSize = 1024;
    private int minBatchSize = 1024;
    /**
     * 一个buffer一旦积累超过多少毫秒(哪怕没有满)也强制flush
     */
    private long batchTime = 1000;
    /**
     * 同一时刻最多允许多少个并发
     */
    private int concurrency = 10;
    /**
     * 大于0的话则限制TPS
     */
    private int tps;
    /**
     * 大于0的话则限制IPS
     */
    private int ips;
    /**
     * worker做的事情通常非常简单, 一个无锁化的线程就可以完成非常多的事情. 除非序列化负担非常重才需要增加workerCount的数量
     */
    private int workerCount = 1;
    /**
     * 关闭writer时最多的等待时间, 单位是秒
     */
    private int closeWaitTime = 30;
    /**
     * ???
     */
    private ThreadFactory threadFactory;
    /**
     * 为true则当容量不足时阻塞, 否则异常
     */
    private boolean blockOnInsufficientCapacity = false;

    // private final

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getMinBatchSize() {
        return minBatchSize;
    }

    public void setMinBatchSize(int minBatchSize) {
        this.minBatchSize = minBatchSize;
    }

    public long getBatchTime() {
        return batchTime;
    }

    public void setBatchTime(long batchTime) {
        this.batchTime = batchTime;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int getTps() {
        return tps;
    }

    public void setTps(int tps) {
        this.tps = tps;
    }

    public int getIps() {
        return ips;
    }

    public void setIps(int ips) {
        this.ips = ips;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }

    public int getCloseWaitTime() {
        return closeWaitTime;
    }

    public void setCloseWaitTime(int closeWaitTime) {
        this.closeWaitTime = closeWaitTime;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }

    public boolean isBlockOnInsufficientCapacity() {
        return blockOnInsufficientCapacity;
    }

    public void setBlockOnInsufficientCapacity(boolean blockOnInsufficientCapacity) {
        this.blockOnInsufficientCapacity = blockOnInsufficientCapacity;
    }

    @Override
    public String toString() {
        return "BatchProcessorProperties{" +
            "queueSize=" + queueSize +
            ", batchSize=" + batchSize +
            ", minBatchSize=" + minBatchSize +
            ", batchTime=" + batchTime +
            ", concurrency=" + concurrency +
            ", tps=" + tps +
            ", ips=" + ips +
            ", workerCount=" + workerCount +
            ", closeWaitTime=" + closeWaitTime +
            ", threadFactory=" + threadFactory +
            ", blockOnInsufficientCapacity=" + blockOnInsufficientCapacity +
            '}';
    }
}
