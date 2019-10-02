package com.xzchaoo.batchprocessor.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
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
@SuppressWarnings("WeakerAccess")
public class DisruptorBatchProcessor<T> implements BatchProcessor<T> {
    private static final Logger                   LOGGER = LoggerFactory.getLogger(DisruptorBatchProcessor.class);
    private static final AtomicInteger            ID = new AtomicInteger();
    private final        BatchProcessorProperties properties;
    private final        Disruptor<Event<T>>      disruptor;
    private final        RingBuffer<Event<T>>     ringBuffer;
    private final        AtomicInteger            index = new AtomicInteger();
    private final        int                      workerCount;
    private final        Limiter                  limiter;
    private final        int                      maxBatchSize;
    private final        AsyncProcessor<T>        reporter;

    public DisruptorBatchProcessor(BatchProcessorProperties properties, AsyncProcessor<T> reporter) {
        this.properties = Objects.requireNonNull(properties);
        this.reporter = Objects.requireNonNull(reporter);
        this.workerCount = properties.getWorkerCount();
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must greater than 0");
        }

        int bufferSize = properties.getQueueSize();
        this.maxBatchSize = Math.min(bufferSize, Math.max(bufferSize / 4, 512));
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

        ExceptionHandler<Event<T>> exceptionHandler = new ExceptionHandler<Event<T>>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, Event<T> event) {
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
        ensureStarted();
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
        ensureStarted();
        if (c == null || c.isEmpty()) {
            return;
        }
        int size = c.size();

        // 太大不可能一口气放进去 只能单个放 或 分小批
        if (size > maxBatchSize) {
            List<T> buffer = new ArrayList<>(maxBatchSize);
            for (T t : c) {
                buffer.add(t);
                if (buffer.size() == maxBatchSize) {
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

        // 这里我们保留最后一个worker的barrier
        SequenceBarrier anyBarrier = null;
        for (int i = 0; i < workerCount; i++) {
            InnerEventHandler<T> handler = new InnerEventHandler<>(i, batchSize, minBatchSize, batchTime, limiter,
                reporter);
            anyBarrier = this.disruptor.handleEventsWith(handler).asSequenceBarrier();
        }
        // disruptor的start方法内部会去启动线程, 而启动线程理论上需要一定的时间
        // 如果在线程启动之前, 就往buffer里put数据, 然后shutdown, 那么这部分数据不会被执行
        // 因此这里的一个策略是进行一次flush(肯定不会flush任何数据), 并且等到sequence 0可用! 这就确保此时最后一个worker已经启动!
        this.disruptor.start();
        this.flush();
        try {
            anyBarrier.waitFor(0);
        } catch (Exception e) {
            throw new IllegalStateException("fail to wait for disruptor to start", e);
        }
    }

    @Override
    public void flush() {
        ensureStarted();
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
        // 先标记为stop, 此后进来的数据会被拒绝
        int oldState = state.getAndSet(2);
        if (oldState != 1) {
            return;
        }
        // 关闭disruptor, 内部实现会等到最新的cursor被消费完
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
            limiter.releaseConcurrency(properties.getConcurrency());
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

    /**
     * 获得底层的disruptor, 但不暴露泛型
     *
     * @return disruptor
     */
    public Disruptor<?> getDisruptor() {
        return disruptor;
    }
}
