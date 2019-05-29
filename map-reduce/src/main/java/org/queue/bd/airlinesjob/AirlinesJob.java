package org.queue.bd.airlinesjob;

import org.apache.hadoop.mapreduce.Job;
import org.queue.bd.commons.Sort;

import java.util.ArrayList;

/**
 * MapReduce job to ...
 */
public class AirlinesJob {

	public static void main(String[] args) throws Exception {

	    final ArrayList<Job> jobs = new ArrayList<>();

		jobs.add(new Summarize(
				"flights-dataset/clean/flights",
				"outputs/map-reduce/airlines/output1")
                .getJob(10, false, false));
		jobs.add(new Join(
		        "outputs/map-reduce/airlines/output1",
                "flights-dataset/clean/airlines",
                "outputs/map-reduce/airlines/output2")
                .getJob(10, false, false));
		jobs.add(new Sort(
		        "outputs/map-reduce/airlines/output2",
                "outputs/map-reduce/airlines/output3")
                .getJob(1, false, false));


		for (Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
	}
}
