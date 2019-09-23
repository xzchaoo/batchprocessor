package com.xzchaoo.batchprocessor.core;


/**
 * @author xzchaoo
 * @date 2019/9/6
 */
class Event<T> {
    T t;
    boolean flush;
    int workerIndex;

    void clear() {
        t = null;
        flush = false;
        workerIndex = 0;
    }

    public T getT() {
        return t;
    }

    public void setT(T t) {
        this.t = t;
    }

    public boolean isFlush() {
        return flush;
    }

    public void setFlush(boolean flush) {
        this.flush = flush;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public void setWorkerIndex(int workerIndex) {
        this.workerIndex = workerIndex;
    }

    @Override
    public String toString() {
        return "Event{" +
            "t=" + t +
            ", flush=" + flush +
            ", workerIndex=" + workerIndex +
            '}';
    }
}
