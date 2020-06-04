package com.xzchaoo.batchprocessor.core.v3;

import java.util.List;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-04
 */
public interface Flusher<T> {
    void flush(List<T> batch, Context context);

    interface Factory<T> {
        Flusher<T> create(int index);
    }

    interface Context {
        /**
         * complete
         */
        void complete();

        int retryCount();

        int maxRetryCount();

        boolean retry();
    }
}
