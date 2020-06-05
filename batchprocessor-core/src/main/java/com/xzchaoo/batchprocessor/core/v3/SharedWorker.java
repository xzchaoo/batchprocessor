package com.xzchaoo.batchprocessor.core.v3;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-05
 */
class SharedWorker<T> extends Worker<T> implements EventHandler<Event<T>> {
    SharedWorker(int index, BatchProcessorConfig config, Flusher<T> flusher, ScheduledExecutorService scheduler, Semaphore semaphore,
                 RingBuffer<Event<T>> ringBuffer) {
        super(index, config, scheduler, semaphore, flusher);
        super.ringBuffer = ringBuffer;
    }
}
