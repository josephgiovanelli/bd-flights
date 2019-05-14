package org.queue.bd.airlinesjob;

import org.apache.hadoop.mapreduce.Job;

import java.util.ArrayList;

/**
 * MapReduce job to ...
 */
public class AirlineJob {

	public static void main(String[] args) throws Exception {

	    ArrayList<Job> jobs = new ArrayList<>();

		jobs.add(new Summarize().getJob());
		jobs.add(new Join().getJob());
		jobs.add(new Sort().getJob());

		for (Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
	}
}
