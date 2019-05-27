package org.queue.bd.airportsjob.richobjects;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import utils.TimeSlot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class RichAirport implements Writable {

    private boolean first;
    private TimeSlot timeSlot;
    private double average;
    private String airport;

    public RichAirport() { }

    public void set(final TimeSlot timeSlot, final double average) {
        this.first = true;
        this.timeSlot = timeSlot;
        this.average = average;
        this.airport = "";
    }

    public void set(final String airport) {
        this.first = false;
        this.timeSlot = null;
        this.average = -1;
        this.airport = airport;
    }

    public boolean isFirst() {
        return first;
    }

    public TimeSlot getTimeSlot() {
        return timeSlot;
    }

    public double getAverage() {
        return average;
    }

    public String getAirport() {
        return airport;
    }

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
