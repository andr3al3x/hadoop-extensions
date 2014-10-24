package org.pentaho.hadoop.mapred.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The Class KettleValueTypeConverter.
 */
public class KettleValueTypeConverter {
    
    /** The log. */
    private final Log log = LogFactory.getLog(KettleValueTypeConverter.class);
    
    /**
     * Gets the string converted value.
     *
     * @param value the value
     * @param columnType the column type
     * @return the string converted value
     */
    public String getStringConvertedValue(byte[] value, HbaseColumnDescriptor.HbaseColumnType columnType) {
        String convertedValue = null;

        switch (columnType) {
        case INTEGER:
            convertedValue = getIntegerToString(value);
            break;
        case NUMBER:
            convertedValue = getNumberToString(value);
            break;
        case STRING:
            convertedValue = Bytes.toString(value);
            break;
        default:
            break;
        }

        return convertedValue;
    }

    /**
     * Gets the integer to string.
     *
     * @param value the value
     * @return the integer to string
     */
    private String getIntegerToString(byte[] value) {
        if (value.length == Bytes.SIZEOF_INT) {
            return Integer.toString(Bytes.toInt(value));
        }

        if (value.length == Bytes.SIZEOF_LONG) {
            return Long.toString(Bytes.toLong(value));
        }

        if (value.length == Bytes.SIZEOF_SHORT) {
            return Short.toString(Bytes.toShort(value));
        }

        log.info("incorrect integer type value of size: " + value.length);
        return null;
    }

    /**
     * Gets the number to string.
     *
     * @param value the value
     * @return the number to string
     */
    private String getNumberToString(byte[] value) {
        if (value.length == Bytes.SIZEOF_FLOAT) {
            return Float.toString(Bytes.toFloat(value));
        }

        if (value.length == Bytes.SIZEOF_DOUBLE) {
            return Double.toString(Bytes.toDouble(value));
        }

        log.info("incorrect number type value of size: " + value.length);
        return null;
    }
}
