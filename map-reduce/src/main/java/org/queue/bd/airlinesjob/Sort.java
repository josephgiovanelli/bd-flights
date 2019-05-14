package org.queue.bd.airlinesjob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.queue.bd.MyJob;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Sort implements MyJob {

    private static final String JOB_NAME = "sort";
    private static final String INPUT_PATH = "output2";
    private static final String OUTPUT_PATH = "output3";

	public static class SortMapper
	extends Mapper<Text, Text, DoubleWritable, Text>{


		public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
		    DoubleWritable average = new DoubleWritable();
		    average.set(Double.parseDouble(value.toString()));
			context.write(average, key);
		}
	}

	public static class SortReducer
	extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
        private Text word = new Text();

		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
			List<String> sortedValues = new LinkedList<>();
			for (Text val : values) {
				sortedValues.add(val.toString());
			}
            Collections.sort(sortedValues);
			//String[] sortedArray = (String[]) sortedValues.toArray();
			//Arrays.sort(sortedArray);
            for (String asortedValues : sortedValues) {
                word.set(asortedValues);
                context.write(word, key);
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
    public Job getJob() throws IOException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, JOB_NAME);

        Path inputPath = new Path(INPUT_PATH);
        Path outputPath = new Path(OUTPUT_PATH);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(Sort.class);
        job.setMapperClass(SortMapper.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setSortComparatorClass(IntComparator.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}