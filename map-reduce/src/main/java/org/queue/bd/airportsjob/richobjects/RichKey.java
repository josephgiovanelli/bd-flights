package org.queue.bd.airportsjob.richobjects;

import org.apache.hadoop.io.WritableComparable;
import utils.TimeSlot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * This is class is used as key between the map and reduce tasks of the SummarizeJob
 */
public class RichKey implements WritableComparable {

    //stores the airports iata code
    private String airport;
    //stores the time slot in which the flight departs
    private TimeSlot timeSlot;

    public RichKey() { }

    /**
     * Sets the airport and the time slot values
     * @param airport the airports iata code
     * @param timeSlot the time slot in which the flight departs
     */
    public void set(final String airport, final TimeSlot timeSlot){
        this.airport = airport;
        this.timeSlot = timeSlot;
    }

    /**
     * Returns the complete airports name
     * @return the complete airports name
     */
    public String getAirport() {
        return airport;
    }

    /**
     * Returns the average of the taxi out delays
     * @return the average of the taxi out delays
     */
    public TimeSlot getTimeSlot() {
        return timeSlot;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(airport.length());
        out.writeChars(airport);
        out.writeInt(timeSlot.ordinal());
    }

    @Override
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
