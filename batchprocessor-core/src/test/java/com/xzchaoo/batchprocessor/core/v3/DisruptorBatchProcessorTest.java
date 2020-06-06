package com.xzchaoo.batchprocessor.core.v3;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

/**
 * created at 2020/6/5
 *
 * @author xzchaoo
 */
public class DisruptorBatchProcessorTest {
    @Test
    public void test() throws InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(4);
        BatchProcessorConfig config = new BatchProcessorConfig();
        config.setWorkerCount(2);
        config.setMaxBatchSize(4);
        config.setMaxConcurrency(2);
        config.setShared(true);
        BatchProcessor<String> p = new DisruptorBatchProcessor<>(config, new Flusher.Factory<String>() {
            @Override
            public Flusher<String> create(int index) {
                return new Flusher<String>() {
                    @Override
                    public void flush(List<String> batch, Context context) {
                        es.execute(() -> {
                            System.out.println(index + " flush " + batch);
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            context.complete();
                        });
                    }
                };
            }
        });
        p.start();
        for (int i = 0; i < 100; i++) {
            p.put("" + i);
        }
        Thread.sleep(10000);
    }
}
