package com.xzchaoo.batchprocessor.core;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * @author xzchaoo
 * @date 2019/9/7
 */
public class DisruptorBatchProcessorTest {
    @Test
    public void test() throws InterruptedException {
        BatchProcessorProperties p = new BatchProcessorProperties();
        // p.setQueueSize(4096);
        p.setBatchSize(1024);
        p.setConcurrency(8);
        p.setWorkerCount(2);
        // p.setIps(5000);
        AtomicInteger finished = new AtomicInteger();
        DisruptorBatchProcessor<Foo> processor = new DisruptorBatchProcessor<>(p, new AsyncProcessorManager<Foo>() {
            @Override
            public AsyncProcessor<Foo> create() {
                return new GrpcAsyncProcessor(finished);
            }

            @Override
            public void shutdown(AsyncProcessor<Foo> asyncProcessor) {
                System.out.println("shutdown AsyncProcessor");
            }
        });
        processor.start();
        long b = System.currentTimeMillis();
        // 65536 / 1024 / 8 * 1 = 8 seconds
        for (int i = 0; i < 65536; i++) {
            processor.put(new Foo());
        }
        processor.stop();
        System.out.println(System.currentTimeMillis() - b);
        System.out.println("关闭了");
        assertThat(System.currentTimeMillis() - b).isBetween(8000L, 9000L);
        assertThat(finished).hasValue(65536);
    }

    @Test
    public void test2() throws InterruptedException {
        BatchProcessorProperties p = new BatchProcessorProperties();
        p.setBatchSize(1024);
        p.setConcurrency(8);
        GrpcAsyncProcessor gap = new GrpcAsyncProcessor();
        DisruptorBatchProcessor<Foo> w = new DisruptorBatchProcessor<>(p, gap);
        w.start();
        w.put(new Foo());
        Thread.sleep(2000);
        w.put(new Foo());
        Thread.sleep(2000);
        w.stop();
        System.out.println("关闭了");
    }
}
