package com.xzchaoo.batchprocessor.core;

import java.util.Collection;

/**
 * @author xzchaoo
 */
public interface BatchProcessor<T> {
    void put(T t);

    void put(Collection<T> c);

    /**
     * 启动
     */
    void start();

    /**
     * 执行一次flush, 通常没有必要
     */
    void flush();

    /**
     * 关闭
     */
    void stop();
}
