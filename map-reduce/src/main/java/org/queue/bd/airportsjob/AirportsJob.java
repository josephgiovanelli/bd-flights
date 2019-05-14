package org.queue.bd.airportsjob;

import org.apache.hadoop.mapreduce.Job;

import java.util.ArrayList;

/**
 * MapReduce job to ...
 */
public class AirportsJob {

	public static void main(String[] args) throws Exception {

	    ArrayList<Job> jobs = new ArrayList<>();

		jobs.add(new Summarize().getJob());
		jobs.add(new Join().getJob());

		for (Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
	}
}
