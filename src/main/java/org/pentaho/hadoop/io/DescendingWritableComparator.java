package org.pentaho.hadoop.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Implementation of WritableComparator in Descending order
 */
public class DescendingWritableComparator extends WritableComparator {

    protected DescendingWritableComparator(Class<? extends WritableComparable> keyClass) {
        super(keyClass);
    }

    /**
     * Override the original method to sort via descending order
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return b.compareTo(a);
    }
}