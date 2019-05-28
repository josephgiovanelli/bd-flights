package org.queue.bd.airlinesjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.queue.bd.MyJob;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Sort implements MyJob {

    private static final String JOB_NAME = "sort";

    private final String inputPath;
    private final String outputPath;

    public Sort(final String inputPath, final String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }


	public static class SortMapper
	extends Mapper<Text, Text, DoubleWritable, Text>{

        private final DoubleWritable average = new DoubleWritable();

        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            average.set(Double.parseDouble(value.toString()));
            context.write(average, key);
		}
	}

	public static class SortReducer
	extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        private Text newKey = new Text();

		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

			final List<String> sortedValues = new LinkedList<>();
			for (Text val : values) {
				sortedValues.add(val.toString());
			}
            Collections.sort(sortedValues);

            for (String sortedValue : sortedValues) {
                newKey.set(sortedValue);
                context.write(newKey, key);
            }
		}
	}

    public static class IntComparator extends WritableComparator {
        public IntComparator() {
            super(IntWritable.class);
        }

        private Integer int1;
        private Integer int2;

        @Override
        public int compare(byte[] raw1, int offset1, int length1, byte[] raw2,
                           int offset2, int length2) {
            int1 = ByteBuffer.wrap(raw1, offset1, length1).getInt();
            int2 = ByteBuffer.wrap(raw2, offset2, length2).getInt();

            return int2.compareTo(int1);
        }

    }

    @Override
    public Job getJob(final int numReducers, final boolean mapOutputCompression, final boolean reduceOutputCompression) throws IOException {

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        conf.set("mapreduce.map.output.compress", String.valueOf(mapOutputCompression));

        Job job = Job.getInstance(conf, JOB_NAME);

        Path inputPath = new Path(this.inputPath);
        Path outputPath = new Path(this.outputPath);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(Sort.class);
        job.setMapperClass(SortMapper.class);

        job.setNumReduceTasks(numReducers);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setSortComparatorClass(IntComparator.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        if (reduceOutputCompression) {
            FileOutputFormat.setCompressOutput(job, reduceOutputCompression);
            FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}