package org.queue.bd.airlinesjob.richobjects;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RichAirline implements Writable {

    private boolean first;
    private String airline;
    private double average;


    public RichAirline() { }

    public void set(final String airline) {
        this.first = false;
        this.airline = airline;
        this.average = -1;
    }

    public void set(final double average) {
        this.first = true;
        this.airline = "";
        this.average = average;
    }

    public boolean isFirst() {
        return first;
    }

    public String getAirline() {
        return airline;
    }

    public double getAverage() {
        return average;
    }

    public void write(DataOutput out) throws IOException {
        out.writeBoolean(first);
        if(first) {
            out.writeDouble(average);
        } else {
            out.writeInt(airline.length());
            out.writeChars(airline);
        }
    }

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
