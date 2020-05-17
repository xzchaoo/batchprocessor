package com.xzchaoo.batchprocessor.core;

import java.util.List;

/**
 * created at 2020/5/17
 *
 * @author xzchaoo
 */
public interface Flusher<T> {
    void flush(List<T> buffer, Context<T> context);

    // void onMissSemaphore(List<T> buffer, Context<T> context);

    interface Context<T> {
        void complete();

        boolean retry(List<T> buffer, long delayMills);
    }
}
