package org.pentaho.hadoop.mapred.hbase;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * The Class HbaseColumnDescriptor.
 */
public class HbaseColumnDescriptor {

    /**
     * The Enum HbaseColumnType.
     */
    public enum HbaseColumnType {

        /** The string. */
        STRING("string"),
        /** The number. */
        NUMBER("number"),
        /** The integer. */
        INTEGER("integer");

        /** The type. */
        private String type;

        /**
         * Instantiates a new hbase column type.
         *
         * @param type the type
         */
        private HbaseColumnType(String type) {
            this.type = type;
        }

        /**
         * Gets the column type.
         *
         * @param columnType the column type
         * @return the column type
         */
        public static HbaseColumnType getColumnType(String columnType) {
            if (columnType.toLowerCase().equals(STRING.type))
                return STRING;

            if (columnType.toLowerCase().equals(NUMBER.type))
                return NUMBER;

            if (columnType.toLowerCase().equals(INTEGER.type))
                return INTEGER;

            return null;
        }
    }

    /** The descriptor delimiter. */
    private static String DESCRIPTOR_DELIMITER = ":";

    /** The family. */
    private byte[] family = null;

    /** The qualifier. */
    private byte[] qualifier = null;

    /** The column type. */
    private HbaseColumnType columnType = HbaseColumnType.STRING;

    /**
     * Instantiates a new hbase column descriptor.
     *
     * @param descriptor the descriptor
     */
    public HbaseColumnDescriptor(String descriptor) {
        String[] splitDescriptor = descriptor.split(DESCRIPTOR_DELIMITER);

        if (splitDescriptor.length == 2) {
            this.family = Bytes.toBytes(splitDescriptor[0]);
            this.qualifier = Bytes.toBytes(splitDescriptor[1]);
        }

        if (splitDescriptor.length == 3) {
            this.columnType = HbaseColumnType.getColumnType(splitDescriptor[3]);
        }
    }

    /**
     * Instantiates a new hbase column descriptor.
     *
     * @param family the family
     * @param qualifier the qualifier
     */
    public HbaseColumnDescriptor(byte[] family, byte[] qualifier) {
        this(family, qualifier, HbaseColumnType.STRING);
    }

    /**
     * Instantiates a new hbase column descriptor.
     *
     * @param family the family
     * @param qualifier the qualifier
     * @param columnType the column type
     */
    public HbaseColumnDescriptor(byte[] family, byte[] qualifier, HbaseColumnType columnType) {
        this.family = family;
        this.qualifier = qualifier;
        this.columnType = columnType;
    }

    /**
     * Gets the family.
     *
     * @return the family
     */
    public byte[] getFamily() {
        return family;
    }

    /**
     * Gets the qualifier.
     *
     * @return the qualifier
     */
    public byte[] getQualifier() {
        return qualifier;
    }

    /**
     * Gets the column type.
     *
     * @return the column type
     */
    public HbaseColumnType getColumnType() {
        return columnType;
    }

    /**
     * Checks if is valid.
     *
     * @return true, if is valid
     */
    public boolean isValid() {
        return (this.family != null) && (this.qualifier != null);
    }
}
