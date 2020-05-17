package com.xzchaoo.batchprocessor.core;

/**
 * @author xzchaoo
 * @date 2019/11/20
 */
public interface AsyncProcessorManager<T> {
    AsyncProcessor<T> create();

    void shutdown(AsyncProcessor<T> asyncProcessor);
}
