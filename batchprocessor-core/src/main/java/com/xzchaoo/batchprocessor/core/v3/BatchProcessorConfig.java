package com.xzchaoo.batchprocessor.core.v3;

import java.time.Duration;

import lombok.Data;

/**
 * created at 2020/6/5
 *
 * @author xzchaoo
 */
@Data
public class BatchProcessorConfig {
    /**
     * Required.
     */
    private String   name;
    private int      workerCount       = 1;
    private int      ringBufferSize    = 65536;
    private int      maxBatchSize      = 1024;
    private int      maxConcurrency    = 16;
    private boolean  flushOnEndOfBatch = false;
    private int      maxRetryCount     = 3;
    private Duration flushInterval     = Duration.ofSeconds(1);
    private boolean  shared            = false;
}
