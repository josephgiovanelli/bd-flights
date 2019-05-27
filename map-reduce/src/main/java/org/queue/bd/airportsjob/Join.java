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
import utils.TimeSlot;
import pojos.Airport;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

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
    	extends Mapper<Text, Text, Text, RichAirport>{

        private final Text airportIataCode = new Text();
        private final RichAirport leftRichAirport = new RichAirport();

        public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

            final String[] richKey = key.toString().split("-");
            airportIataCode.set(richKey[0]);
            leftRichAirport.set(TimeSlot.getTimeSlot(Integer.parseInt(richKey[1])), Double.parseDouble(value.toString()));
		    context.write(airportIataCode, leftRichAirport);
		}
		
	}
	
	/**
	 * Mapper for airlines dataset
	 */
	public static class SecondMapper
	extends Mapper<LongWritable, Text, Text, RichAirport>{

        private final Text airportIataCode = new Text();
        private final RichAirport rightRichAirport = new RichAirport();

        public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

            Airport airport = new Airport(value.toString());
            airportIataCode.set(airport.getIata_code());
            rightRichAirport.set(airport.getAirport());
            context.write(airportIataCode, rightRichAirport);
        }
		
	}

	/**
	 * Reducer
	 */
	public static class JobReducer
	    extends Reducer<Text, RichAirport, Text, DoubleWritable> {

	    private final Text joinedAirport = new Text();
	    private final DoubleWritable average = new DoubleWritable();

        public void reduce(Text key, Iterable<RichAirport> values, Context context)
				throws IOException, InterruptedException {

            final List<RichAirport> richAirportsValues = new LinkedList<>();
            final List<RichAirport> richAirportsKeys = new LinkedList<>();

            for (RichAirport val : values) {
                if (val.isFirst()) {
                    RichAirport copy = new RichAirport();
                    copy.set(val.getTimeSlot(), val.getAverage());
                    richAirportsValues.add(copy);
                } else {
                    RichAirport copy = new RichAirport();
                    copy.set(val.getAirport());
                    richAirportsKeys.add(copy);
                }
            }

			for (RichAirport richAirportKey : richAirportsKeys) {
                for (RichAirport richAirportValue: richAirportsValues) {
                    joinedAirport.set(richAirportKey.getAirport() + "," + richAirportValue.getTimeSlot().getDescription());
                    average.set(richAirportValue.getAverage());
                    context.write(joinedAirport, average);
                }
            }
        }
	}

    @Override
    public Job getJob(final int numReducers, final boolean lzo) throws IOException {

        Configuration conf = new Configuration();
        conf.set("mapred.compress.map.output", String.valueOf(lzo));

        Job job = Job.getInstance(conf, JOB_NAME);

        Path firstInputPath = new Path(this.firstInputPath);
        Path secondInputPath = new Path(this.secondInputPath);
        Path outputPath = new Path(this.outputPath);

        MultipleInputs.addInputPath(job, firstInputPath, KeyValueTextInputFormat.class, FirstMapper.class);
        MultipleInputs.addInputPath(job, secondInputPath, TextInputFormat.class, SecondMapper.class);


        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(Join.class);

        //job.setNumReduceTasks(numReducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RichAirport.class);
        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}
