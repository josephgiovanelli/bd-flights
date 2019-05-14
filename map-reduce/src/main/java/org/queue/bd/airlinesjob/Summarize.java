package org.queue.bd.airlinesjob;

import org.queue.bd.MyJob;
import org.queue.bd.richobjects.RichAverage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pojos.Flight;

import java.io.IOException;

public class Summarize implements MyJob {

    private static final String JOB_NAME = "summarize";
    private static final String INPUT_PATH = "flights/flights.csv";
    private static final String OUTPUT_PATH = "output1";

    public static class SummarizeMapper
	extends Mapper<LongWritable, Text, Text, RichAverage>{

		public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
			final Flight flight = new Flight(value.toString());
			if (flight.getCancelled().equals("0") && flight.getDiverted().equals("0")) {
                final RichAverage richAverage = new RichAverage(Integer.parseInt(flight.getArrival_delay()), 1);
                context.write(new Text(flight.getAirline()), richAverage);
            }
		}
	}

	public static class SummarizeReducer
	extends Reducer<Text, RichAverage, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<RichAverage> values, Context context)
                throws IOException, InterruptedException {
			double totalSum = 0;
			double totalCount = 0;
			for (RichAverage val : values) {
				totalSum += val.getSum();
				totalCount += val.getCount();
			}
			result.set(totalSum / totalCount);
			context.write(key, result);
		}
	}

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, JOB_NAME);

        Path inputPath = new Path(INPUT_PATH);
        Path outputPath = new Path(OUTPUT_PATH);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(Summarize.class);
        job.setMapperClass(SummarizeMapper.class);

        //job.setNumReduceTasks(NUM_REDUCERS);

        job.setReducerClass(SummarizeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RichAverage.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}