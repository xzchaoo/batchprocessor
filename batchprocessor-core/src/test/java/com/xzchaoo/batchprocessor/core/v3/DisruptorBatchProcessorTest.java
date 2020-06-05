package com.xzchaoo.batchprocessor.core.v3;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * created at 2020/6/5
 *
 * @author xzchaoo
 */
public class DisruptorBatchProcessorTest {
    @Test
    public void test() throws InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(4);
        BatchProcessor<String> p = new DisruptorBatchProcessor<>(1, 4, 2, new Flusher.Factory<String>() {
            @Override
            public Flusher<String> create(int index) {
                return new Flusher<String>() {
                    @Override
                    public void flush(List<String> batch, Context context) {
                        es.execute(() -> {
                            System.out.println("flush " + batch);
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