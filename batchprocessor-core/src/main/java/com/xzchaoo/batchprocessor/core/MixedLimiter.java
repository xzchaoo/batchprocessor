package com.xzchaoo.batchprocessor.core;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author xzchaoo
 */
public class MixedLimiter implements Limiter {
    private final Semaphore concurrency;
    private final RateLimiter tps;
    private final RateLimiter ips;

    public MixedLimiter(int concurrency, int tps, int ips) {
        this.concurrency = new Semaphore(concurrency);
        if (tps > 0) {
            this.tps = RateLimiter.create(tps);
        } else {
            this.tps = null;
        }
        if (ips > 0) {
            this.ips = RateLimiter.create(ips);
        } else {
            this.ips = null;
        }
    }

    @Override
    public void acquireConcurrency() throws InterruptedException {
        concurrency.acquire();
    }

    @Override
    public void acquireRate(int size) {
        if (tps != null) {
            tps.acquire();
        }
        if (ips != null) {
            ips.acquire(size);
        }
    }

    @Override
    public boolean tryAcquireConcurrency(int size, int seconds) throws InterruptedException {
        return concurrency.tryAcquire(size, seconds, TimeUnit.SECONDS);
    }

    @Override
    public void releaseConcurrency() {
        concurrency.release();
    }

    @Override
    public void releaseConcurrency(int size) {
        concurrency.release(size);
    }
}
