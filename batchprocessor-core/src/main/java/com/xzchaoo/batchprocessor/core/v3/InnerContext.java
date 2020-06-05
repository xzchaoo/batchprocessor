package com.xzchaoo.batchprocessor.core.v3;

/**
 * created at 2020/6/5
 *
 * @author xzchaoo
 */
public class InnerContext<T> implements Flusher.Context {
    @Override
    public void complete() {

    }

    @Override
    public int retryCount() {
        return 0;
    }

    @Override
    public int maxRetryCount() {
        return 0;
    }

    @Override
    public boolean retry() {
        return false;
    }
}
