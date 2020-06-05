package com.xzchaoo.batchprocessor.core.v3;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-04
 */
public interface BatchProcessor<T> {
    int workerCount();

    void start();

    void stop(boolean waitForAllToComplete);

    boolean tryPut(T t);

    void put(T t);
}
