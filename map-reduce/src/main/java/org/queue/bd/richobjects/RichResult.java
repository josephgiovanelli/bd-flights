package org.queue.bd.richobjects;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RichResult implements WritableComparable {

    private String airline;
    private double average;

    public RichResult() { }

    public RichResult(final String airline, final double average) {
        this.airline = airline;
        this.average = average;
    }

    public String getAirline() {
        return airline;
    }

    public double getAverage() {
        return average;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(airline.length());
        out.writeChars(airline);
        out.writeDouble(average);
    }

    public void readFields(DataInput in) throws IOException {
        final int airlineLength = in.readInt();
        for (int i = 0; i < airlineLength; i++) {
            airline += in.readChar();
        }
        average = in.readInt();
    }

    @Override
    public int compareTo(Object o){
        if (this.airline.compareTo(((RichResult) o).airline) == 0) {
            return Double.compare(this.average, ((RichResult) o).average);
        }
        return this.airline.compareTo(((RichResult) o).airline);
    }

}
