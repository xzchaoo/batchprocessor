package com.xzchaoo.batchprocessor.core.v2;

import java.util.List;

/**
 * created at 2020/5/17
 *
 * @author xzchaoo
 */
public interface Flusher<T> {
    void flush(List<T> batch, Context<T> context);

    interface Factory<T> {
        Flusher<T> create(int index);
    }

    /**
     * 能支持超时吗?
     *
     * @param <T>
     */
    interface Context<T> {
        void complete();

        void retry(long delayMills, List<T> batch);
    }
}
