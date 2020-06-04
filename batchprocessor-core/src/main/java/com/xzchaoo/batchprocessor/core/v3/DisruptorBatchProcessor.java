package com.xzchaoo.batchprocessor.core.v3;

import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.xzchaoo.batchprocessor.core.v2.SimpleThreadFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-04
 */
public class DisruptorBatchProcessor<T> implements BatchProcessor<T> {

    private static final int ACTION_ADD = 0;
    private static final int ACTION_FLUSH = 1;

    private final Disruptor<Event<T>> disruptor;
    private final RingBuffer<Event<T>> ringBuffer;
    private int maxBatchSize = 1024;
    private int maxConcurrency = 10;
    private int wip;
    private ArrayDeque<List<T>> delayed = new ArrayDeque<>(65536);
    private boolean flushOnEndOfBeach = false;
    private final Flusher<T> flusher;

    DisruptorBatchProcessor(int workerCount, Flusher.Factory<T> factory) {
        flusher = factory.create(0);
        // for (int i = 0; i < workerCount; i++) {
        // }
        // this.factory = factory;
        disruptor = new Disruptor<>(
            Event::new,
            65536,
            new SimpleThreadFactory("test"),
            ProducerType.MULTI,
            new LiteBlockingWaitStrategy());
        // disruptor.setDefaultExceptionHandler();
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> onEvent(event, endOfBatch))
            // for gc
            .then((event, sequence, endOfBatch) -> event.clear());
        ringBuffer = disruptor.getRingBuffer();
    }

    private final List<T> buffer = new ArrayList<>(100);

    private void onEvent(Event<T> event, boolean endOfBatch) {
        buffer.add(event.payload);
        if (buffer.size() == maxBatchSize) {
            flush(buffer, true);
        }
    }

    private void flush(List<T> buffer, boolean clear) {
        try {
            List<T> copy = new ArrayList<>(buffer);
            if (wip < maxConcurrency) {
                ++wip;
                flusher.flush(copy);
            } else {
                delayed.add(copy);
            }
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
