package org.queue.bd.richobjects;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is class is used as value between the map and reduce tasks of the SummarizeJob(s)
 */
public class RichSum implements WritableComparable {

    //stores the sum of the elements to be averaged
    private int sum;
    //stores the number of the elements to be averaged
    private int count;

    public RichSum() { }

    /**
     * Returns the sum of the elements
     * @return the sum of the elements
     */
    public int getSum() {
        return sum;
    }

    /**
     * Returns the number of the elements
     * @return the number of the elements
     */
    public int getCount() {
        return count;
    }

    /**
     * Sets the fields of the class
     * @param sum the sum of the elements
     * @param count the number of the elements
     */
    public void set(final int sum, final int count){
        this.sum = sum;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
        out.writeInt(count);
    }

    @Override
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
