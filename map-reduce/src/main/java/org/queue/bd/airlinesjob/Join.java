package org.queue.bd.airlinesjob;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
    private static final String FIRST_INPUT_PATH = "airlines/output1";
    private static final String SECOND_INPUT_PATH = "flights/airlines.csv";
    private static final String OUTPUT_PATH = "airlines/output2";
		
	/**
	 * Mapper for Summarize job
	 */
	public static class FirstMapper
    	extends Mapper<Text, Text, Text, RichAirline>{

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
		    context.write(key, new RichAirline(Double.parseDouble(value.toString())));
		}
		
	}
	
	/**
	 * Mapper for airlines dataset
	 */
	public static class SecondMapper
	extends Mapper<LongWritable, Text, Text, RichAirline>{

        public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

            Airline airline = new Airline(value.toString());
            context.write(new Text(airline.getIata_code()), new RichAirline(airline.getAirline()));
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
			
			/*List<RichAirline> firstDatasetRecords = new ArrayList<>();
			List<RichAirline> secondDatasetRecords = new ArrayList<>();*/

			for(RichAirline val : values) {
				if (val.isFirst()) {
				    //firstDatasetRecords.add(val);
                    average.set(val.getAverage());
                } else {
				    //secondDatasetRecords.add(val);
                    airline.set(val.getAirline());
                }
			}
			/*for(RichAirline first : firstDatasetRecords) {
				for(RichAirline second : secondDatasetRecords) {
                    context.write(new Text(second.getAirline()), new DoubleWritable(first.getAverage()));
				}		 
			}*/
            context.write(airline, average);
        }
	 
	}

    @Override
    public Job getJob() throws IOException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, JOB_NAME);

        Path firstInputPath = new Path(FIRST_INPUT_PATH);
        Path secondInputPath = new Path(SECOND_INPUT_PATH);
        Path outputPath = new Path(OUTPUT_PATH);

        MultipleInputs.addInputPath(job, firstInputPath, KeyValueTextInputFormat.class, FirstMapper.class);
        MultipleInputs.addInputPath(job, secondInputPath, TextInputFormat.class, SecondMapper.class);


        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(Join.class);

        //job.setNumReduceTasks(NUM_REDUCERS);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RichAirline.class);
        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}
