package org.queue.bd.airportsjob;

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
import org.queue.bd.MyJob;
import org.queue.bd.airportsjob.richobjects.RichAirport;
import org.queue.bd.richobjects.RichAverage;
import pojos.Flight;

import java.io.IOException;

public class Summarize implements MyJob {

    private static final String JOB_NAME = "summarize";
    private static final String INPUT_PATH = "flights/flights.csv";
    private static final String OUTPUT_PATH = "airports/output1";

    public static class SummarizeMapper
	extends Mapper<LongWritable, Text, RichAirport, RichAverage>{

		public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
			final Flight flight = new Flight(value.toString());
			if (flight.getCancelled().equals("0") && flight.getDiverted().equals("0")) {
                final RichAverage richAverage = new RichAverage(Integer.parseInt(flight.getTaxi_out()), 1);
                final RichAirport richAirport = new RichAirport(flight.getOrigin_airport(),
                        RichAirport.getTimeSlot(flight.getScheduled_departure()));
                context.write(richAirport, richAverage);
            }
		}
	}

	public static class SummarizeReducer
	extends Reducer<RichAirport, RichAverage, RichAirport, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(RichAirport key, Iterable<RichAverage> values, Context context)
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
        job.setMapOutputKeyClass(RichAirport.class);
        job.setMapOutputValueClass(RichAverage.class);
        job.setOutputKeyClass(RichAirport.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}