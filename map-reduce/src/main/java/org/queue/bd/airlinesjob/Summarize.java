package org.queue.bd.airlinesjob;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.queue.bd.MyJob;
import org.queue.bd.richobjects.RichSum;
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

    private final String inputPath;
    private final String outputPath;

    public Summarize(final String inputPath, final String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static class SummarizeMapper
	extends Mapper<LongWritable, Text, Text, RichSum>{

        private final Text airline = new Text();
        private final RichSum richSum = new RichSum();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

			final Flight flight = new Flight(value.toString());
            richSum.set(Integer.parseInt(flight.getArrival_delay()), 1);
            airline.set(flight.getAirline());
            context.write(airline, richSum);
		}
	}

    public static class RichSumCombiner
    extends Reducer<Text, RichSum, Text, RichSum> {

        private final RichSum richSum = new RichSum();

        public void reduce(Text key, Iterable<RichSum> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            int count = 0;
            for (RichSum value : values) {
                sum += value.getSum();
                count += value.getCount();
            }
            richSum.set(sum, count);
            context.write(key, richSum);
        }
    }

	public static class SummarizeReducer
	extends Reducer<Text, RichSum, Text, DoubleWritable> {

		private final DoubleWritable average = new DoubleWritable();

		public void reduce(Text key, Iterable<RichSum> values, Context context)
                throws IOException, InterruptedException {

			double totalSum = 0;
			double totalCount = 0;
			for (RichSum val : values) {
				totalSum += val.getSum();
				totalCount += val.getCount();
			}
			average.set(totalSum / totalCount);
			context.write(key, average);
		}
	}

    @Override
    public Job getJob(final int numReducers, final boolean mapOutputCompression, final boolean reduceOutputCompression) throws IOException {

        Configuration conf = new Configuration();
        conf.set("mapreduce.map.output.compress", String.valueOf(mapOutputCompression));

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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RichSum.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (reduceOutputCompression) {
            FileOutputFormat.setCompressOutput(job, reduceOutputCompression);
            FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}