package com.xzchaoo.batchprocessor.core.v3;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.xzchaoo.batchprocessor.core.v2.OneThreadFactory;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-05
 */
class StandaloneWorker<T> extends Worker<T> implements EventHandler<Event<T>> {

    private Disruptor<Event<T>> disruptor;

    StandaloneWorker(int index, BatchProcessorConfig config, ScheduledExecutorService scheduler, Semaphore semaphore, Flusher<T> flusher) {
        super(index, config, scheduler, semaphore, flusher);
    }

    @Override
    void start() {
        disruptor = new Disruptor<>(
                Event::new,
                config.getRingBufferSize(),
                new OneThreadFactory(config.getName() + "-" + index),
                ProducerType.MULTI,
                new LiteBlockingWaitStrategy());
        // disruptor.setDefaultExceptionHandler();
        disruptor.handleEventsWith(this);
        super.ringBuffer = disruptor.getRingBuffer();
        super.start();
        disruptor.start();
    }

    @Override
    void stop(boolean waitForAllToComplete) {
        super.stop(waitForAllToComplete);
        try {
            disruptor.shutdown(config.getCloseWaitTimeoutMills(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException("close timeout", e);
        }
    }
}
