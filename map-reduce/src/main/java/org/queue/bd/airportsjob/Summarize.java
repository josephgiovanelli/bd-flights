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
import org.queue.bd.airportsjob.richobjects.RichKey;
import org.queue.bd.airportsjob.richobjects.TimeSlot;
import org.queue.bd.richobjects.RichSum;
import pojos.Flight;

import java.io.IOException;

public class Summarize implements MyJob {

    private static final String JOB_NAME = "summarize";
    private static final String INPUT_PATH = "flights/flights.csv";
    private static final String OUTPUT_PATH = "airports/output1";

    public static class SummarizeMapper
	extends Mapper<LongWritable, Text, RichKey, RichSum>{

		public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
			final Flight flight = new Flight(value.toString());
			if (flight.getCancelled().equals("0") && flight.getDiverted().equals("0") && flight.getOrigin_airport().length() == 3) {
                final RichSum richSum = new RichSum(Integer.parseInt(flight.getTaxi_out()), 1);
                final RichKey richKey = new RichKey(flight.getOrigin_airport(),
                        TimeSlot.getTimeSlot(flight.getScheduled_departure()));
                context.write(richKey, richSum);
            }
		}
	}

    public static class RichSumCombiner
            extends Reducer<RichKey, RichSum, RichKey, RichSum> {

        public void reduce(RichKey key, Iterable<RichSum> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (RichSum value : values) {
                sum += value.getSum();
                count += value.getCount();
            }
            context.write(key, new RichSum(sum, count));
        }
    }

	public static class SummarizeReducer
	extends Reducer<RichKey, RichSum, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		private Text richKey = new Text();

		public void reduce(RichKey key, Iterable<RichSum> values, Context context)
                throws IOException, InterruptedException {
			double totalSum = 0;
			double totalCount = 0;
			for (RichSum val : values) {
				totalSum += val.getSum();
				totalCount += val.getCount();
			}
			result.set(totalSum / totalCount);
            richKey.set(key.toString());
			context.write(richKey, result);
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

        job.setCombinerClass(RichSumCombiner.class);
        job.setReducerClass(SummarizeReducer.class);
        job.setMapOutputKeyClass(RichKey.class);
        job.setMapOutputValueClass(RichSum.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}