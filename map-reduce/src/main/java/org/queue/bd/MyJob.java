package org.queue.bd;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public interface MyJob {
    Job getJob(final int numReducers, final boolean lzo) throws IOException;
}
