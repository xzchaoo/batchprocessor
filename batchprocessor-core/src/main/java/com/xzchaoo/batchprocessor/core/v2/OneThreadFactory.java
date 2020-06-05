package com.xzchaoo.batchprocessor.core.v2;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-05
 */
public class OneThreadFactory implements ThreadFactory {
    private final String name;

    public OneThreadFactory(String name) {
        this.name = Objects.requireNonNull(name);
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, name);
    }
}
