package com.xzchaoo.batchprocessor.core.v2;

import java.util.List;

/**
 * created at 2020/5/17
 *
 * @author xzchaoo
 */
public interface BatchProcessor<T> {
    /**
     * try to put an item to buffer, will return false if buffer is full
     *
     * @param t item
     * @return true if success else false
     */
    boolean tryPut(T t);

    /**
     * put an item to buffer, will block if buffer is full
     *
     * @param t
     */
    void put(T t);

    /**
     * try to put an collection items as a whole to buffer, will return false if buffer is full
     *
     * @param coll
     * @return
     */
    boolean tryPut(List<T> coll);

    /**
     * try to put an collection items to buffer, will block if buffer is full
     *
     * @param coll
     */
    void put(List<T> coll);

    void flush();

    /**
     * start
     */
    void start();

    /**
     * stop
     */
    void stop();
}
