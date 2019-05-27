package org.queue.bd.richobjects;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RichSum implements WritableComparable {

    private int sum;
    private int count;

    public RichSum() { }

    public int getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public void set(final int sum, final int count){
        this.sum = sum;
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
        out.writeInt(count);
    }
    public void readFields(DataInput in) throws IOException {
        sum = in.readInt();
        count = in.readInt();
    }


    @Override
    public int compareTo(Object o){
        double presentValue = this.sum / this.count;
        double compareValue = ((RichSum) o).sum / ((RichSum) o).count;
        return Double.compare(presentValue, compareValue);
    }
}
