package org.queue.bd.airportsjob.richobjects;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import utils.TimeSlot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * This is class is used as value between the map and reduce tasks of the JoinJob
 */
public class RichAirport implements Writable {

    //discriminates the instances produced by the two mappers
    private boolean first;
    //stores the time slot in which the flight departs
    private TimeSlot timeSlot;
    //stores the average of the taxi out delays
    private double average;
    //stores the complete airports name
    private String airport;

    public RichAirport() { }

    /**
     * Sets the time slot and the average and marks the object as produced by the mapper that reads the SummarizeJob output
     * @param timeSlot the time slot in which the flight departs
     * @param average the taxi out average delay
     */
    public void set(final TimeSlot timeSlot, final double average) {
        this.first = true;
        this.timeSlot = timeSlot;
        this.average = average;
        this.airport = "";
    }

    /**
     * Sets the airport name and marks the object as produced by the mapper that reads from the airports.csv file
     * @param airport the name of the airport
     */
    public void set(final String airport) {
        this.first = false;
        this.timeSlot = null;
        this.average = -1;
        this.airport = airport;
    }

    /**
     * Returns the mapper discriminator
     * @return true if it comes from the SummarizeJob, false otherwise
     */
    public boolean isFirst() {
        return first;
    }

    /**
     * Returns the time slot in which the flight departs
     * @return the time slot in which the flight departs
     */
    public TimeSlot getTimeSlot() {
        return timeSlot;
    }

    /**
     * Returns the average of the taxi out delays
     * @return the average of the taxi out delays
     */
    public double getAverage() {
        return average;
    }

    /**
     * Returns the complete airports name
     * @return the complete airports name
     */
    public String getAirport() {
        return airport;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(first);
        if(first) {
            out.writeInt(timeSlot.ordinal());
            out.writeDouble(average);
        } else {
            out.writeInt(airport.length());
            out.writeChars(airport);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readBoolean();
        if (first) {
            timeSlot = TimeSlot.getTimeSlot(in.readInt());
            average = in.readDouble();
            airport = "";
        } else {
            timeSlot = null;
            average = -1;
            airport = "";
            final int airportLength = in.readInt();
            for (int i = 0; i < airportLength; i++) {
                airport = airport + in.readChar();
            }
        }
    }
}
