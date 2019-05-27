package org.queue.bd.airportsjob.richobjects;

import org.apache.hadoop.io.WritableComparable;
import utils.TimeSlot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class RichKey implements WritableComparable {

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

    @Override
    public int compareTo(Object o) {
        RichKey other = (RichKey) o;
        if (this.airport.equals(other.airport) && this.timeSlot.equals(other.timeSlot)) return 0;
        if (!this.airport.equals(other.airport)) return this.airport.compareTo(other.airport);
        else return this.timeSlot.compareTo(other.timeSlot);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RichKey richKey = (RichKey) o;
        return Objects.equals(airport, richKey.airport) &&
                timeSlot == richKey.timeSlot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(airport, timeSlot.ordinal());
    }
}
