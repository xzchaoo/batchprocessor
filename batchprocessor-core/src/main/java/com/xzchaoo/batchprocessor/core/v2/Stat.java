package com.xzchaoo.batchprocessor.core.v2;

import java.util.ArrayList;
import java.util.List;

/**
 * created at 2020/5/23
 *
 * @author xzchaoo
 */
public class Stat {
    String state;
    int usedBufferSize;
    int totalBufferSize;
    SemaphoreStat semaphoreStat = new SemaphoreStat();
    List<WorkerStat> workers = new ArrayList<>();

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public List<WorkerStat> getWorkers() {
        return workers;
    }

    public void setWorkers(List<WorkerStat> workers) {
        this.workers = workers;
    }

    public SemaphoreStat getSemaphoreStat() {
        return semaphoreStat;
    }

    public void setSemaphoreStat(SemaphoreStat semaphoreStat) {
        this.semaphoreStat = semaphoreStat;
    }

    public int getUsedBufferSize() {
        return usedBufferSize;
    }

    public void setUsedBufferSize(int usedBufferSize) {
        this.usedBufferSize = usedBufferSize;
    }

    public int getTotalBufferSize() {
        return totalBufferSize;
    }

    public void setTotalBufferSize(int totalBufferSize) {
        this.totalBufferSize = totalBufferSize;
    }

    public static class WorkerStat {
        int usedBufferSize;
        int totalBufferSize;

        public int getUsedBufferSize() {
            return usedBufferSize;
        }

        public void setUsedBufferSize(int usedBufferSize) {
            this.usedBufferSize = usedBufferSize;
        }

        public int getTotalBufferSize() {
            return totalBufferSize;
        }

        public void setTotalBufferSize(int totalBufferSize) {
            this.totalBufferSize = totalBufferSize;
        }

        @Override
        public String toString() {
            return "WorkerStat{" +
                "usedBufferSize=" + usedBufferSize +
                ", totalBufferSize=" + totalBufferSize +
                '}';
        }
    }

    public static class SemaphoreStat {
        int maxPermits;
        int availablePermits;
        int queuedSize;

        public int getMaxPermits() {
            return maxPermits;
        }

        public void setMaxPermits(int maxPermits) {
            this.maxPermits = maxPermits;
        }

        public int getAvailablePermits() {
            return availablePermits;
        }

        public void setAvailablePermits(int availablePermits) {
            this.availablePermits = availablePermits;
        }

        public int getQueuedSize() {
            return queuedSize;
        }

        public void setQueuedSize(int queuedSize) {
            this.queuedSize = queuedSize;
        }

        @Override
        public String toString() {
            return "SemaphoreStat{" +
                "maxPermits=" + maxPermits +
                ", availablePermits=" + availablePermits +
                ", queuedSize=" + queuedSize +
                '}';
        }
    }

    @Override
    public String toString() {
        return "Stat{" +
            "state='" + state + '\'' +
            ", usedBufferSize=" + usedBufferSize +
            ", totalBufferSize=" + totalBufferSize +
            ", semaphoreStat=" + semaphoreStat +
            ", workers=" + workers +
            '}';
    }
}
