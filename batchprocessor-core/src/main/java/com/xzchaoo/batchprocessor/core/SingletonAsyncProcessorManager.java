package com.xzchaoo.batchprocessor.core;

import java.util.Objects;

/**
 * @author xzchaoo
 * @since 1.2.0
 */
public class SingletonAsyncProcessorManager<T> implements AsyncProcessorManager<T> {
    private final AsyncProcessor<T> asyncProcessor;

    public SingletonAsyncProcessorManager(AsyncProcessor<T> asyncProcessor) {
        this.asyncProcessor = Objects.requireNonNull(asyncProcessor);
    }

    @Override
    public AsyncProcessor<T> create() {
        return asyncProcessor;
    }

    @Override
    public void shutdown(AsyncProcessor<T> asyncProcessor) {

    }
}
