package com.xzchaoo.batchprocessor.core;

/**
 * 限制并发数 和 TPS 和 IPS
 *
 * @author xzchaoo
 */
public interface Limiter {
    void acquireConcurrency() throws InterruptedException;

    boolean tryAcquireConcurrency(int size, int seconds) throws InterruptedException;

    void releaseConcurrency();

    void releaseConcurrency(int size);

    void acquireRate(int size);
}
