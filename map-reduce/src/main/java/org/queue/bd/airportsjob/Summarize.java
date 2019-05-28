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

        final RichSum richSum = new RichSum();
        final RichKey richKey = new RichKey();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

			final Flight flight = new Flight(value.toString());
            richSum.set(Integer.parseInt(flight.getTaxi_out()), 1);
            richKey.set(flight.getOrigin_airport(), TimeSlot.getTimeSlot(flight.getDeparture_time()));
            context.write(richKey, richSum);
		}
	}

    public static class RichSumCombiner
            extends Reducer<RichKey, RichSum, RichKey, RichSum> {

        final RichSum richSum = new RichSum();
        final RichKey richKey = new RichKey();

        public void reduce(RichKey key, Iterable<RichSum> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            int count = 0;
            for (RichSum value : values) {
                sum += value.getSum();
                count += value.getCount();
            }
            richKey.set(key.getAirport(), key.getTimeSlot());
            richSum.set(sum, count);
            context.write(richKey, richSum);
        }
    }

	public static class SummarizeReducer
	extends Reducer<RichKey, RichSum, Text, DoubleWritable> {

        private final DoubleWritable average = new DoubleWritable();
        private final Text richKey = new Text();

        public void reduce(RichKey key, Iterable<RichSum> values, Context context)
                throws IOException, InterruptedException {

			double totalSum = 0;
			double totalCount = 0;
			for (RichSum val : values) {
				totalSum += val.getSum();
				totalCount += val.getCount();
			}
            average.set(totalSum / totalCount);
			richKey.set(key.toString());
            context.write(richKey, average);
		}
	}

    @Override
    public Job getJob(final int numReducers, final boolean lzo) throws IOException {

        Configuration conf = new Configuration();
        conf.set("mapreduce.map.output.compress", String.valueOf(lzo));

        Job job = Job.getInstance(conf, JOB_NAME);

        Path inputPath = new Path(this.inputPath);
        Path outputPath = new Path(this.outputPath);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(Summarize.class);
        job.setMapperClass(SummarizeMapper.class);

        job.setNumReduceTasks(numReducers);

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