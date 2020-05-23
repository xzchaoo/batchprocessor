package com.xzchaoo.batchprocessor.core.v2;

/**
 * created at 2020/5/17
 *
 * @author xzchaoo
 */
class Event<T> {
    T payload;
    Object arg1;
    Object arg2;
    int arg_int;

    void clear() {
        payload = null;
        arg1 = null;
        arg2 = null;
        arg_int = 0;
    }
}
