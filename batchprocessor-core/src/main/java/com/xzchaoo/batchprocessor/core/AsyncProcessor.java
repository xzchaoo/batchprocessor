package com.xzchaoo.batchprocessor.core;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * @author xzchaoo
 */
public interface AsyncProcessor<T> {
    /**
     * 异步处理一批, 实现者不能缓存batch的应用, 因为它会变化
     *
     * @param batch 批次
     * @return future
     * @throws Exception any exception
     */
    ListenableFuture<?> asyncProcess(List<T> batch) throws Exception;
}
