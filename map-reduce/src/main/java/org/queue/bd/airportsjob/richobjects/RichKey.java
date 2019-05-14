package org.queue.bd.airportsjob.richobjects;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RichKey implements Writable {

    private String airport;
    private TimeSlot timeSlot;

    public RichKey() { }

    public RichKey(final String airport, final TimeSlot timeSlot){
        this.airport = airport;
        this.timeSlot = timeSlot;
    }

    public String getAirport() {
        return airport;
    }

    public TimeSlot getTimeSlot() {
        return timeSlot;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(airport.length());
        out.writeChars(airport);
        out.writeInt(timeSlot.ordinal());
    }
    public void readFields(DataInput in) throws IOException {

        airport = "";
        final int airportLength = in.readInt();
        for (int i = 0; i < airportLength; i++) {
            airport = airport + in.readChar();
        }
        timeSlot = TimeSlot.getTimeSlot(in.readInt());
    }

    @Override
    public String toString() {
        return airport + "-" + timeSlot.ordinal();
    }
}
