package airlinesjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * MapReduce job to join yellowTaxi.seq and zone.seq.
 */
public class AirlineJob {

	
	public static void main(String[] args) throws Exception {

		ArrayList<Job> jobs = new ArrayList<>();
		
		jobs.add( Job.getInstance(new Configuration(), "First job") );
		jobs.add( Job.getInstance(new Configuration(), "Second job") );
		
		// Output path and data type of first job 
		// should be the same as the input ones of the second job  

		for (Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
		
		// Do not use the old, deprecated APIs with job2.addDependingJob(job1)
	 
	}
}
