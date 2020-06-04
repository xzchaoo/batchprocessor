package com.xzchaoo.batchprocessor.core.v3;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.xzchaoo.batchprocessor.core.v2.SimpleThreadFactory;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-04
 */
public class DisruptorBatchProcessor<T> implements BatchProcessor<T> {

    private static final int ACTION_ADD   = 0;
    private static final int ACTION_FLUSH = 1;

    private final Disruptor<Event<T>>  disruptor;
    private final RingBuffer<Event<T>> ringBuffer;
    private       int                  maxBatchSize   = 1024;
    private       int                  maxConcurrency = 10;
    private       int                  wip;
    private       ArrayDeque<T>        delayed        = new ArrayDeque<>(65536);

    DisruptorBatchProcessor() {
        disruptor = new Disruptor<>(
                Event::new,
                65536,
                new SimpleThreadFactory("test"),
                ProducerType.MULTI,
                new LiteBlockingWaitStrategy());
        // disruptor.setDefaultExceptionHandler();
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> onEvent(event))
                // for gc
                .then((event, sequence, endOfBatch) -> event.clear());
        ringBuffer = disruptor.getRingBuffer();
    }

    private List<T> buffer = new ArrayList<>();

    private void onEvent(Event<T> event) {
        buffer.add(event.payload);
        if (buffer.size() == maxBatchSize) {
            flush(buffer, true);
        }
    }

    private void flush(List<T> buffer, boolean clear) {
        try {

        } finally {
            if (clear) {
                buffer.clear();
            }
        }
    }

    @Override
    public void start() {
        disruptor.start();
    }

    @Override
    public void stop() {
        disruptor.shutdown();
    }

    @Override
    public boolean tryPut(T t) {
        return false;
    }

    @Override
    public void put(T t) {
        if (t == null) {
            throw new NullPointerException();
        }
        long next = ringBuffer.next();
        Event<T> event = ringBuffer.get(next);
        event.payload = t;
    }
}
