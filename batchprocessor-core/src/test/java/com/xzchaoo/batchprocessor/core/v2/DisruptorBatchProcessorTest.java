package com.xzchaoo.batchprocessor.core.v2;

import org.junit.Test;

import java.util.Arrays;

/**
 * created at 2020/5/17
 *
 * @author xzchaoo
 */
public class DisruptorBatchProcessorTest {
    @Test
    public void test() throws InterruptedException {
        BatchProcessorConfig config = new BatchProcessorConfig();
        config.setName("foo");
        config.setFlusherFactory(index -> (batch, context) -> {
            if (context.retryCount() > 1) {
                System.out.println("flush " + batch);
                context.complete();
            } else {
                context.retry(1000, batch);
            }
        });
        config.setConcurrency(2);
        config.setFlushSize(10);
        DisruptorBatchProcessor<String> p = new DisruptorBatchProcessor<>(config);
        p.start();
        for (int i = 0; i < 3; i++) {
            p.put("1");
            p.put("1");
            p.put(Arrays.asList("2", "3"));
            p.put(Arrays.asList("2", "3"));
            p.flush();
        }
        for (int i = 0; i < 3; i++) {
            Thread.sleep(1000);
            System.out.println(p.stat());
        }
        p.stop();
    }
}