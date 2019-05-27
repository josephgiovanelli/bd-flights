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
import utils.TimeSlot;
import org.queue.bd.richobjects.RichSum;
import pojos.Flight;

import java.io.IOException;

public class Summarize implements MyJob {

    private static final String JOB_NAME = "summarize";

    private final String inputPath;
    private final String outputPath;

    public Summarize(final String inputPath, final String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static class SummarizeMapper
	extends Mapper<LongWritable, Text, RichKey, RichSum>{

		public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
			final Flight flight = new Flight(value.toString());
            final RichSum richSum = new RichSum(Integer.parseInt(flight.getTaxi_out()), 1);
            final RichKey richKey = new RichKey(flight.getOrigin_airport(),
                    TimeSlot.getTimeSlot(flight.getDeparture_time()));
            context.write(richKey, richSum);
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
            context.write(new RichKey(key.getAirport(), key.getTimeSlot()), new RichSum(sum, count));
        }
    }

	public static class SummarizeReducer
	extends Reducer<RichKey, RichSum, Text, DoubleWritable> {

		public void reduce(RichKey key, Iterable<RichSum> values, Context context)
                throws IOException, InterruptedException {
			double totalSum = 0;
			double totalCount = 0;
			for (RichSum val : values) {
				totalSum += val.getSum();
				totalCount += val.getCount();
			}
			context.write(new Text(key.toString()), new DoubleWritable(totalSum / totalCount));
		}
	}

    @Override
    public Job getJob() throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, JOB_NAME);

        Path inputPath = new Path(this.inputPath);
        Path outputPath = new Path(this.outputPath);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(Summarize.class);
        job.setMapperClass(SummarizeMapper.class);

        //job.setNumReduceTasks(1);

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