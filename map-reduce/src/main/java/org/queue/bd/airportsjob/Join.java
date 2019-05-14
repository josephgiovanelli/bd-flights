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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.queue.bd.MyJob;
import org.queue.bd.airportsjob.richobjects.RichAirport;
import org.queue.bd.airportsjob.richobjects.TimeSlot;
import pojos.Airport;

import java.io.IOException;

/**
 * MapReduce job to join Summarize job and airlines.csv.
 */
public class Join implements MyJob {

    private static final String JOB_NAME = "join";
    private static final String FIRST_INPUT_PATH = "airports/output1";
    private static final String SECOND_INPUT_PATH = "flights/airports.csv";
    private static final String OUTPUT_PATH = "airports/output2";
		
	/**
	 * Mapper for Summarize job
	 */
	public static class FirstMapper
    	extends Mapper<Text, Text, Text, RichAirport>{

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
            final String[] richKey = key.toString().split("-");
		    context.write(new Text(richKey[0]), new RichAirport(TimeSlot.getTimeSlot(Integer.parseInt(richKey[1])),
                    Double.parseDouble(value.toString())));
		}
		
	}
	
	/**
	 * Mapper for airlines dataset
	 */
	public static class SecondMapper
	extends Mapper<LongWritable, Text, Text, RichAirport>{

        public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
            Airport airport = new Airport(value.toString());
            context.write(new Text(airport.getIata_code()), new RichAirport(airport.getAirport()));
		}
		
	}

	/**
	 * Reducer
	 */
	public static class JobReducer
	    extends Reducer<Text, RichAirport, Text, DoubleWritable> {


        Text richKey = new Text();
        DoubleWritable average = new DoubleWritable();

        public void reduce(Text key, Iterable<RichAirport> values, Context context)
				throws IOException, InterruptedException {
			
			/*List<RichAirport> firstDatasetRecords = new ArrayList<>();
			List<RichAirport> secondDatasetRecords = new ArrayList<>();*/

			TimeSlot timeSlot = null;
			String airport = "";

			for(RichAirport val : values) {
				if (val.isFirst()) {
				    //firstDatasetRecords.add(val);
                    timeSlot = val.getTimeSlot();
                    average.set(val.getAverage());
                } else {
				    //secondDatasetRecords.add(val);
                    airport = val.getAirport();
                }
			}
			/*for(RichAirport first : firstDatasetRecords) {
				for(RichAirport second : secondDatasetRecords) {
                    context.write(new Text(second.getAirline()), new DoubleWritable(first.getAverage()));
				}		 
			}*/
			richKey.set(airport + "-" + timeSlot.ordinal());
            context.write(richKey, average);
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
        job.setMapOutputValueClass(RichAirport.class);
        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}
