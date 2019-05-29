package org.queue.bd.airlinesjob;

import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.queue.bd.MyJob;
import org.queue.bd.airlinesjob.richobjects.RichAirline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pojos.Airline;

import java.io.IOException;

/**
 * MapReduce job to join Summarize job and airlines.csv.
 */
public class Join implements MyJob {

    private static final String JOB_NAME = "join";

    private final String firstInputPath;
    private final String secondInputPath;
    private final String outputPath;

    public Join(final String firstInputPath, final String secondInputPath, final String outputPath) {
        this.firstInputPath = firstInputPath;
        this.secondInputPath = secondInputPath;
        this.outputPath = outputPath;
    }

    /**
	 * Mapper for Summarize job
	 */
	public static class FirstMapper
    	extends Mapper<Text, DoubleWritable, Text, RichAirline>{

	    private final RichAirline leftRichAirline = new RichAirline();

		public void map(Text key, DoubleWritable value, Context context)
				throws IOException, InterruptedException {
		    leftRichAirline.set(value.get());
		    context.write(key, leftRichAirline);
		}
		
	}
	
	/**
	 * Mapper for airlines dataset
	 */
	public static class SecondMapper
	extends Mapper<LongWritable, Text, Text, RichAirline>{

	    private final Text iataCode = new Text();
        private final RichAirline rightRichAirline = new RichAirline();

        public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

            Airline airline = new Airline(value.toString());
            iataCode.set(airline.getIata_code());
            rightRichAirline.set(airline.getAirline());
            context.write(iataCode, rightRichAirline);
		}
		
	}

	/**
	 * Reducer
	 */
	public static class JobReducer
	    extends Reducer<Text, RichAirline, Text, DoubleWritable> {


        Text airline = new Text();
        DoubleWritable average = new DoubleWritable();

        public void reduce(Text key, Iterable<RichAirline> values, Context context)
				throws IOException, InterruptedException {

			for(RichAirline val : values) {
				if (val.isFirst()) {
                    average.set(val.getAverage());
                } else {
                    airline.set(val.getAirline());
                }
			}
            context.write(airline, average);
        }
	 
	}

    @Override
    public Job getJob(final int numReducers, final boolean mapOutputCompression, final boolean reduceOutputCompression) throws IOException {

        Configuration conf = new Configuration();
        conf.set("mapreduce.map.output.compress", String.valueOf(mapOutputCompression));

        Job job = Job.getInstance(conf, JOB_NAME);

        Path firstInputPath = new Path(this.firstInputPath);
        Path secondInputPath = new Path(this.secondInputPath);
        Path outputPath = new Path(this.outputPath);

        MultipleInputs.addInputPath(job, firstInputPath, SequenceFileInputFormat.class, FirstMapper.class);
        MultipleInputs.addInputPath(job, secondInputPath, TextInputFormat.class, SecondMapper.class);


        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(Join.class);

        job.setNumReduceTasks(numReducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RichAirline.class);
        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (reduceOutputCompression) {
            FileOutputFormat.setCompressOutput(job, reduceOutputCompression);
            FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}
