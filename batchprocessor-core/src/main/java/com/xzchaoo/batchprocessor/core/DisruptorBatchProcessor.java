package com.xzchaoo.batchprocessor.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xzchaoo
 */
public class DisruptorBatchProcessor<T> implements BatchProcessor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorBatchProcessor.class);
    private static final AtomicInteger ID = new AtomicInteger();
    private final DisruptorBufferWriterProperties properties;
    private final Disruptor<Event<T>> disruptor;
    private final RingBuffer<Event<T>> ringBuffer;
    private final AtomicInteger index = new AtomicInteger();
    private final int workerCount;
    private final Limiter limiter;
    private final int maxBatchSize;
    private final int splitBatchSize;
    private final AsyncProcessor<T> reporter;

    public DisruptorBatchProcessor(DisruptorBufferWriterProperties properties, AsyncProcessor<T> reporter) {
        this.properties = Objects.requireNonNull(properties);
        this.reporter = Objects.requireNonNull(reporter);
        int bufferSize = properties.getQueueSize();
        this.maxBatchSize = Math.min(bufferSize, Math.max(bufferSize / 4, 512));
        this.splitBatchSize = Math.min(maxBatchSize, Math.max(bufferSize / 16, 128));
        ThreadFactory threadFactory = properties.getThreadFactory() != null ? properties.getThreadFactory() :
            new ThreadFactoryBuilder()
                .setNameFormat("DisruptorBatchProcessor-" + ID.getAndIncrement() + "-%d")
                .build();

        this.disruptor = new Disruptor<>(
            Event::new,
            bufferSize,
            threadFactory,
            ProducerType.MULTI,
            new LiteTimeoutBlockingWaitStrategy(properties.getBatchTime(), TimeUnit.MILLISECONDS));

        this.ringBuffer = this.disruptor.getRingBuffer();

        this.limiter = new MixedLimiter(properties.getConcurrency(), properties.getTps(), properties.getIps());
        this.workerCount = properties.getWorkerCount();

        ExceptionHandler<Event<T>> exceptionHandler = new ExceptionHandler<Event<T>>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, Event<T> event) {
                // 几乎不太可能
                LOGGER.error("handleEventException {}", event, ex);
            }

            @Override
            public void handleOnStartException(Throwable ex) {
                LOGGER.error("handleOnStartException", ex);
            }

            @Override
            public void handleOnShutdownException(Throwable ex) {
                LOGGER.error("handleOnShutdownException", ex);
            }
        };
        this.disruptor.setDefaultExceptionHandler(exceptionHandler);
    }

    private int nextWorkerIndex() {
        return workerCount == 1 ? 0 : ((index.getAndIncrement() & 0X7FFF_FFFF) % workerCount);
    }

    @Override
    public void put(T t) {
        if (t == null) {
            return;
        }
        long cursor;
        if (properties.isBlockOnInsufficientCapacity()) {
            cursor = ringBuffer.next();
        } else {
            try {
                cursor = this.ringBuffer.tryNext();
            } catch (InsufficientCapacityException e) {
                throw new IllegalStateException(e);
            }
        }
        Event<T> event = ringBuffer.get(cursor);
        event.clear();
        event.t = t;
        event.workerIndex = nextWorkerIndex();
        this.ringBuffer.publish(cursor);
    }

    @Override
    public void put(Collection<T> c) {
        if (c == null || c.isEmpty()) {
            return;
        }
        int size = c.size();

        // 太大不可能一口气放进去 只能单个放 或 分小批
        if (size > maxBatchSize) {
            List<T> buffer = new ArrayList<>(splitBatchSize);
            for (T t : c) {
                buffer.add(t);
                if (buffer.size() == splitBatchSize) {
                    put(buffer);
                    buffer.clear();
                }
            }
            if (!buffer.isEmpty()) {
                put(buffer);
                buffer.clear();
            }
            return;
        }

        long cursor;
        if (properties.isBlockOnInsufficientCapacity()) {
            cursor = this.ringBuffer.next(size);
        } else {
            try {
                cursor = this.ringBuffer.tryNext(size);
            } catch (InsufficientCapacityException e) {
                throw new IllegalStateException(e);
            }
        }
        long i = cursor;
        for (T t : c) {
            Event<T> event = this.ringBuffer.get(i);
            event.clear();
            event.t = t;
            event.workerIndex = nextWorkerIndex();
            ++i;
        }
        this.ringBuffer.publish(cursor, cursor + size - 1);
    }

    private final AtomicInteger state = new AtomicInteger(0);

    @Override
    public void start() {
        if (!state.compareAndSet(0, 1)) {
            throw new IllegalStateException("DisruptorBatchProcessor is already started");
        }
        long batchTime = properties.getBatchTime();
        int batchSize = properties.getBatchSize();
        int minBatchSize = properties.getMinBatchSize();
        for (int i = 0; i < workerCount; i++) {
            InnerEventHandler<T> handler = new InnerEventHandler<>(i, batchSize, minBatchSize, batchTime, limiter,
                reporter);
            this.disruptor.handleEventsWith(handler);
        }
        this.disruptor.start();
    }

    @Override
    public void flush() {
        for (int i = 0; i < workerCount; i++) {
            this.ringBuffer.publishEvent((event, sequence, workerIndex) -> {
                event.clear();
                event.flush = true;
                event.workerIndex = workerIndex;
            }, i);
        }
    }

    @Override
    public void stop() {
        int oldState = state.getAndSet(2);
        if (oldState != 1) {
            return;
        }
        try {
            disruptor.shutdown(properties.getCloseWaitTime(), TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException("shutdown disruptor error", e);
        }

        // 此时我们可以保证不会再有请求流入, 但可能还有正在异步执行中的请求
        // 等到信号量归零可以保证此时已经没有这样的请求了
        try {
            boolean acquired = limiter.tryAcquireConcurrency(properties.getConcurrency(),
                properties.getCloseWaitTime());
            if (!acquired) {
                // 依旧有正在执行的请求 此时只能异常了
                throw new IllegalStateException("依旧有正在执行的请求");
            }
            limiter.releaseConcurrency();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void ensureStarted() {
        int state = this.state.get();
        if (state != 1) {
            if (state == 0) {
                throw new IllegalStateException("DisruptorBatchProcessor is not yet started");
            } else {
                throw new IllegalStateException("DisruptorBatchProcessor is already stopped");
            }
        }
    }

    public Disruptor<?> getDisruptor() {
        return disruptor;
    }
}
