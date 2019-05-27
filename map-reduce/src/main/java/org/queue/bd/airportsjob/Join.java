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
import java.util.Iterator;
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

        private Text newKey = new Text();
        //private Text newValue = new Text();

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
            final String[] richKey = key.toString().split("-");
            newKey.set(richKey[0]);
            //newValue.set("0;" + richKey[1] + "-" + value.toString());
		    context.write(newKey, new RichAirport(TimeSlot.getTimeSlotFromDescription(richKey[1]), Double.parseDouble(value.toString())));
		    newKey.clear();
		    //newValue.clear();
		}
		
	}
	
	/**
	 * Mapper for airlines dataset
	 */
	public static class SecondMapper
	extends Mapper<LongWritable, Text, Text, RichAirport>{

        private Text newKey = new Text();
        //private Text newValue = new Text();

        public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
            Airport airport = new Airport(value.toString());
            newKey.set(airport.getIata_code());
            //newValue.set("1;" + airport.getAirport());
            context.write(newKey, new RichAirport(airport.getAirport()));
            newKey.clear();
            //newValue.clear();
        }
		
	}

	/**
	 * Reducer
	 */
	public static class JobReducer
	    extends Reducer<Text, RichAirport, Text, Text> {

        private Text newKey = new Text();
        private Text newValue = new Text();


        public void reduce(Text key, Iterable<RichAirport> values, Context context)
				throws IOException, InterruptedException {

            String airport = "";
            /*List<String> richAirports = new LinkedList<>();
            for (Text value : values) {
                String[] richAirport = value.toString().split(";");
                if (richAirport[0].equals("1")) {
                    airport = richAirport[1];
                } else {
                    richAirports.add(richAirport[1]);
                }
            }*/
            final List<RichAirport> richAirportsValues = new LinkedList<>();
            final List<RichAirport> richAirportsKeys = new LinkedList<>();

            Iterator<RichAirport> temps = values.iterator();
            while (temps.hasNext()) {
                RichAirport val = temps.next();
                if (val.isFirst()) {
                    RichAirport copy = new RichAirport(val.getTimeSlot(), val.getAverage());
                    richAirportsValues.add(copy);
                } else {
                    RichAirport copy = new RichAirport(new String(val.getAirport()));
                    richAirportsKeys.add(copy);
                    //newKey.set(copy.getAirport());
                }
            }

			//StringBuilder result = new StringBuilder();
			/*for(String YArichAirport : richAirports) {
                //result.append(richAirport).append(",");
                newKey.set(airport);
                newValue.set(YArichAirport);
                context.write(newKey, newValue);
            }*/
			for (RichAirport richAirportKey : richAirportsKeys) {
                for (RichAirport richAirportValue: richAirportsValues) {
                    //result.append(richAirport.getTimeSlot()).append("-").append(richAirport.getAverage());
                    //newValue.set(result.toString());
                    context.write(new Text(richAirportKey.getAirport()), new Text(richAirportValue.getTimeSlot().getDescription() + "-" + String.valueOf(richAirportValue.getAverage())));
                    //newKey.clear();
                    //newValue.clear();
                }
            }

            /*result = new StringBuilder(new StringBuilder(result.substring(0, result.length() - 1)));
            newKey.set(airport);
            newValue.set(result.toString());
            context.write(newKey, newValue);
            newKey.clear();
            newValue.clear();*/


        }
	 
	}

    @Override
    public Job getJob() throws IOException {

        Configuration conf = new Configuration();
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

        //job.setNumReduceTasks(NUM_REDUCERS);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RichAirport.class);
        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}
