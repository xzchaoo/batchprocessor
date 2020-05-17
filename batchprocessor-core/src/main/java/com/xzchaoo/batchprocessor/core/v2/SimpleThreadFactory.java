package com.xzchaoo.batchprocessor.core.v2;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * created at 2020/5/17
 *
 * @author xzchaoo
 */
public class SimpleThreadFactory implements ThreadFactory {
    private final String namePrefix;
    private final AtomicInteger index = new AtomicInteger();

    public SimpleThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        String name = namePrefix + "-" + (index.getAndIncrement());
        return new Thread(r, name);
    }
}
