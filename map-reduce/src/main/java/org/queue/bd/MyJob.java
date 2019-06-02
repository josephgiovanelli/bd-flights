package org.queue.bd;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Common interface that every job class has to implement
 */
public interface MyJob {

    /**
     * Obtain the job instance with the given parameters
     * @param numReducers number of reducers
     * @param mapOutputCompression enable map output compression
     * @param reduceOutputCompression enable reduce output compression
     * @return the instance
     * @throws IOException
     */
    Job getJob(final int numReducers, final boolean mapOutputCompression, final boolean reduceOutputCompression) throws IOException;
}
