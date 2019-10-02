package com.xzchaoo.batchprocessor.core;

import org.junit.Test;

/**
 * @author xzchaoo
 * @date 2019/9/7
 */
public class DisruptorBufferWriterTest {
    @Test
    public void test() throws InterruptedException {
        BatchProcessorProperties p = new BatchProcessorProperties();
        // p.setQueueSize(4096);
        p.setBatchSize(1024);
        p.setConcurrency(8);
        // p.setIps(5000);
        DisruptorBatchProcessor<Foo> processor = new DisruptorBatchProcessor<>(p, new GrpcAsyncProcessor());
        processor.start();
        long b = System.currentTimeMillis();
        for (int i = 0; i < 65536; i++) {
            processor.put(new Foo());
        }
        processor.stop();
        System.out.println(System.currentTimeMillis() - b);
        System.out.println("关闭了");
    }

    @Test
    public void test2() throws InterruptedException {
        BatchProcessorProperties p = new BatchProcessorProperties();
        p.setBatchSize(1024);
        p.setConcurrency(8);
        DisruptorBatchProcessor<Foo> w = new DisruptorBatchProcessor<>(p, new GrpcAsyncProcessor());
        w.start();
        w.put(new Foo());
        Thread.sleep(2000);
        w.put(new Foo());
        Thread.sleep(2000);
        w.stop();
        System.out.println("关闭了");
    }
}
