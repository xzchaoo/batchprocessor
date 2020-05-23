package com.xzchaoo.batchprocessor.core.v2;

import java.util.List;
import java.util.Objects;

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

    class Singleton<T> implements Flusher.Factory<T> {
        private final Flusher<T> flusher;

        public Singleton(Flusher<T> flusher) {
            this.flusher = Objects.requireNonNull(flusher);
        }

        public static <T> Flusher.Factory<T> of(Flusher<T> flusher) {
            return new Singleton<>(flusher);
        }

        @Override
        public Flusher create(int index) {
            return flusher;
        }
    }

    /**
     * 能支持超时吗?
     *
     * @param <T>
     */
    interface Context<T> {
        /**
         * if this flush method call is retry, the return value will greater than zero
         *
         * @return
         */
        int retryCount();

        /**
         * max retry count
         *
         * @return
         */
        int maxRetryCount();

        void complete();

        void retry(long delayMills, List<T> batch);
    }
}
