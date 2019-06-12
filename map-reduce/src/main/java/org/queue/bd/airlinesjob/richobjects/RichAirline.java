package org.queue.bd.airlinesjob.richobjects;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is class is used as value between the map and reduce tasks of the JoinJob
 */
public class RichAirline implements Writable {

    //discriminates the instances produced by the two mappers
    private boolean first;
    //stores the complete airline name
    private String airline;
    //stores the average of the arrival delay
    private double average;

    public RichAirline() { }

    /**
     * Sets the airline name and marks the object as produced by the mapper that reads from the airlines.csv file
     * @param airline the name of the airline
     */
    public void set(final String airline) {
        this.first = false;
        this.airline = airline;
        this.average = -1;
    }

    /**
     * Sets the average and marks the object as produced by the mapper that reads the SummarizeJob output
     * @param average the average delay
     */
    public void set(final double average) {
        this.first = true;
        this.airline = "";
        this.average = average;
    }

    /**
     * Returns the mapper discriminator
     * @return true if it comes from the SummarizeJob, false otherwise
     */
    public boolean isFirst() {
        return first;
    }

    /**
     * Returns the airline name
     * @return the name of the airline
     */
    public String getAirline() {
        return airline;
    }

    /**
     * Returns the average delay
     * @return the average delay
     */
    public double getAverage() {
        return average;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(first);
        if(first) {
            out.writeDouble(average);
        } else {
            out.writeInt(airline.length());
            out.writeChars(airline);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readBoolean();
        if (first) {
            average = in.readDouble();
            airline = "";
        } else {
            average = -1;
            airline = "";
            final int airlineLength = in.readInt();
            for (int i = 0; i < airlineLength; i++) {
                airline = airline + in.readChar();
            }
        }
    }

}
