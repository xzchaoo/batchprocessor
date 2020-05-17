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
        config.setFlushSize(10);
        config.setConcurrency(2);
        config.setFlusherFactory((Flusher.Factory<String>) index -> (Flusher<String>) (batch, context) -> {
            System.out.println("flush " + batch);
            context.complete();
        });
        DisruptorBatchProcessor<String> p = new DisruptorBatchProcessor<>(config);
        p.start();
        for (int i = 0; i < 3; i++) {
            p.put("1");
            p.put("1");
            p.put(Arrays.asList("2", "3"));
            p.put(Arrays.asList("2", "3"));
            p.flush();
        }
        p.stop();
    }
}