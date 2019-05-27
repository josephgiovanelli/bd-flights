package org.queue.bd.airportsjob.richobjects;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import utils.TimeSlot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class RichAirport implements WritableComparable {

    private boolean first;
    private TimeSlot timeSlot;
    private double average;
    private String airport;

    public RichAirport() { }

    public RichAirport(final TimeSlot timeSlot, final double average) {
        this.first = true;
        this.timeSlot = timeSlot;
        this.average = average;
        this.airport = "";
    }

    public RichAirport(final String airport) {
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
            out.writeInt(timeSlot.getDescription().length());
            out.writeChars(timeSlot.getDescription());
            out.writeDouble(average);
        } else {
            out.writeInt(airport.length());
            out.writeChars(airport);
        }
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readBoolean();
        if (first) {
            String timeSlotDescription = "";
            final int timeSlotDescriptionLength = in.readInt();
            for (int i = 0; i < timeSlotDescriptionLength; i++) {
                timeSlotDescription = timeSlotDescription + in.readChar();
            }
            timeSlot = TimeSlot.getTimeSlotFromDescription(timeSlotDescription);
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

    @Override
    public int compareTo(Object o) {
        RichAirport other = (RichAirport) o;
        if(this.first && other.first) {
            return Double.compare(this.average, other.average);
        } else if(!this.first && !other.first) {
            return this.airport.compareTo(other.airport);
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RichAirport that = (RichAirport) o;
        return first == that.first &&
                Double.compare(that.average, average) == 0 &&
                timeSlot == that.timeSlot &&
                Objects.equals(airport, that.airport);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, timeSlot, average, airport);
    }
}
