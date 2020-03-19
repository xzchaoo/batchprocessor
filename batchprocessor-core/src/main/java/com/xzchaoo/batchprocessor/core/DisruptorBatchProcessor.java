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
    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorBatchProcessor.class);
    private static final int STATE_NOT_INIT = 0;
    private static final int STATE_RUNNING = 1;
    private static final int STATE_STOPPED = 2;
    private static final AtomicInteger ID = new AtomicInteger();
    private final BatchProcessorProperties properties;
    private final Disruptor<Event<T>> disruptor;
    private final RingBuffer<Event<T>> ringBuffer;
    private final AtomicInteger index = new AtomicInteger();
    private final int workerCount;
    private final int workerMask;
    private final Limiter limiter;
    private final int maxBatchSize;
    private final AsyncProcessorManager<T> asyncProcessorManager;
    private final List<AsyncProcessor<T>> asyncProcessors = new ArrayList<>();
    private final AtomicInteger state = new AtomicInteger(0);

    public DisruptorBatchProcessor(BatchProcessorProperties properties, AsyncProcessor<T> reporter) {
        this(properties, new SingletonAsyncProcessorManager<>(reporter));
    }

    public DisruptorBatchProcessor(BatchProcessorProperties properties,
                                   AsyncProcessorManager<T> asyncProcessorManager) {
        this.properties = Objects.requireNonNull(properties);
        this.asyncProcessorManager = Objects.requireNonNull(asyncProcessorManager);
        this.workerCount = properties.getWorkerCount();
        // https://www.geeksforgeeks.org/program-to-find-whether-a-no-is-power-of-two/

        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must greater than 0");
        }
        if (!isPowerOfTwo(workerCount)) {
            throw new IllegalArgumentException("workerCount must be power of 2");
        }
        workerMask = workerCount - 1;

        int bufferSize = properties.getQueueSize();
        this.maxBatchSize = Math.min(bufferSize, Math.max(bufferSize / 8, 512));
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

    /**
     * see <a href="https://www.geeksforgeeks.org/program-to-find-whether-a-no-is-power-of-two/">this doc</a>
     *
     * @param x
     * @return
     */
    private static boolean isPowerOfTwo(int x) {
        return x != 0 && ((x & (x - 1)) == 0);

    }

    private int nextWorkerIndex() {
        return workerCount == 1 ? 0 : (index.getAndIncrement() & workerMask);
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
    public boolean tryPut(T t) {
        ensureStarted();
        if (t == null) {
            return false;
        }
        long cursor;
        try {
            cursor = this.ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            return false;
        }
        Event<T> event = ringBuffer.get(cursor);
        event.clear();
        event.t = t;
        event.workerIndex = nextWorkerIndex();
        this.ringBuffer.publish(cursor);
        return true;
    }

    @Override
    public boolean tryPut(Collection<T> c) {
        ensureStarted();
        if (c == null || c.isEmpty()) {
            return true;
        }
        int size = c.size();
        long hi;
        try {
            hi = this.ringBuffer.tryNext(size);
        } catch (InsufficientCapacityException e) {
            throw new IllegalStateException(e);
        }
        put0(c, hi - size + 1, hi);
        return true;
    }

    @Override
    public void put(Collection<T> c) {
        ensureStarted();
        if (c == null || c.isEmpty()) {
            return;
        }
        int size = c.size();

        // 太大不可能一口气放进去 只能单个放 或 分小批
        // TODO 这个有风险 可能部分已经放进去了
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

        long hi;
        if (properties.isBlockOnInsufficientCapacity()) {
            hi = this.ringBuffer.next(size);
        } else {
            try {
                hi = this.ringBuffer.tryNext(size);
            } catch (InsufficientCapacityException e) {
                throw new IllegalStateException(e);
            }
        }
        put0(c, hi - size + 1, hi);
    }

    private void put0(Collection<T> c, long lo, long hi) {
        long i = lo;
        int workerIndex = nextWorkerIndex();
        for (T t : c) {
            Event<T> event = this.ringBuffer.get(i);
            event.clear();
            event.t = t;
            event.workerIndex = (workerIndex++) & workerMask;
            ++i;
        }
        this.ringBuffer.publish(lo, hi);
    }

    @Override
    public void start() {
        if (!state.compareAndSet(STATE_NOT_INIT, STATE_RUNNING)) {
            throw new IllegalStateException("DisruptorBatchProcessor is already started");
        }
        long batchTime = properties.getBatchTime();
        int batchSize = properties.getBatchSize();
        int minBatchSize = properties.getMinBatchSize();

        // 这里我们保留最后一个worker的barrier
        SequenceBarrier anyBarrier = null;
        for (int i = 0; i < workerCount; i++) {
            // 记录并关闭
            AsyncProcessor<T> asyncProcessor = asyncProcessorManager.create();
            asyncProcessors.add(asyncProcessor);
            InnerEventHandler<T> handler = new InnerEventHandler<>(i, batchSize, minBatchSize, batchTime, limiter,
                asyncProcessor);
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
        int oldState = state.getAndSet(STATE_STOPPED);
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
            for (AsyncProcessor<T> asyncProcessor : asyncProcessors) {
                asyncProcessorManager.shutdown(asyncProcessor);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void ensureStarted() {
        int state = this.state.get();
        if (state != STATE_RUNNING) {
            if (state == STATE_NOT_INIT) {
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
