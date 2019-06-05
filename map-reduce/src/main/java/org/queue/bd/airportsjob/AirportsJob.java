package org.queue.bd.airportsjob;

import org.apache.hadoop.mapreduce.Job;
import org.queue.bd.commons.Sort;

import java.util.ArrayList;

/**
 * MapReduce job to ...
 */
public class AirportsJob {

	public static void main(String[] args) throws Exception {

	    final ArrayList<Job> jobs = new ArrayList<>();

        jobs.add(new Summarize(
                "flights-dataset/clean/flights",
                "outputs/map-reduce/airports/output1")
                .getJob(1, true, true));
        jobs.add(new Join(
                "outputs/map-reduce/airports/output1",
                "flights-dataset/clean/airports",
                "outputs/map-reduce/airports/output2")
                .getJob(1, false, false));
		jobs.add(new Sort(
		        "outputs/map-reduce/airports/output2",
                "outputs/map-reduce/airports/output3")
                .getJob(1, false, false));


		for (Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
	}
}
