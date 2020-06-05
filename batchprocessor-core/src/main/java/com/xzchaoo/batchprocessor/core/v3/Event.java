package com.xzchaoo.batchprocessor.core.v3;

/**
 * @author xiangfeng.xzc
 * @date 2020-06-04
 */
class Event<T> {
    int index = -1;
    int action;
    T   payload;

    void clear() {
        index = -1;
        action = 0;
        payload = null;
    }
}
