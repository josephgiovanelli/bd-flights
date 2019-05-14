package org.queue.bd.airportsjob.richobjects;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.queue.bd.richobjects.RichAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

enum TimeSlot {

    MORNING("Morning"),
    AFTERNOON("Afternoon"),
    EVENING("Evening"),
    NIGHT("Night");

    private final String description;

    TimeSlot(final String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }
}

public class RichAirport implements Writable {

    private String airport;
    private TimeSlot timeSlot;

    public RichAirport() { }

    public RichAirport(final String airport, final TimeSlot timeSlot){
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
        timeSlot = TimeSlot.values()[in.readInt()];
    }

    public static TimeSlot getTimeSlot(final String time) {
        final int hour = Integer.parseInt(time.substring(0, 2));
        if (hour >= 0 && hour < 6) return TimeSlot.NIGHT;
        if (hour >= 6 && hour < 12) return TimeSlot.MORNING;
        if (hour >= 12 && hour < 18) return TimeSlot.AFTERNOON;
        return TimeSlot.EVENING;
    }
}
