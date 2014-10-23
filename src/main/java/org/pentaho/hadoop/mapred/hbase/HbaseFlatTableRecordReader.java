package org.pentaho.hadoop.mapred.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

// TODO: Auto-generated Javadoc
/**
 * The Class HbaseFlatTableRecordReader.
 */
public class HbaseFlatTableRecordReader implements RecordReader<Text, Text> {

    /** The log. */
    private final Log log = LogFactory.getLog(HbaseFlatTableRecordReader.class);

    /** The scanner. */
    private ResultScanner scanner;

    /** The input column descriptors. */
    private List<HbaseColumnDescriptor> inputColumnDescriptors;

    /** The start row. */
    private byte[] startRow;

    /** The end row. */
    private byte[] endRow;

    /** The htable. */
    private HTable htable;

    /** The scan row cache size. */
    private int scanRowCacheSize;

    /** The column delimiter. */
    private String columnDelimiter;

    /**
     * Inits the.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void init() throws IOException {
        Scan scan = new Scan(startRow, endRow);
        scan.setCacheBlocks(false);
        scan.setCaching(scanRowCacheSize);

        configureScanWithInputColumns(scan);

        this.scanner = htable.getScanner(scan);
    }

    /**
     * Configure scan with input columns.
     *
     * @param scan the scan
     */
    protected void configureScanWithInputColumns(Scan scan) {
        for (HbaseColumnDescriptor hcd : inputColumnDescriptors) {
            scan.addColumn(hcd.getFamily(), hcd.getQualifier());
        }
    }

    /**
     * Sets the input column descriptors.
     *
     * @param inputColumnDescriptors the new input column descriptors
     */
    public void setInputColumnDescriptors(List<HbaseColumnDescriptor> inputColumnDescriptors) {
        this.inputColumnDescriptors = inputColumnDescriptors;
    }

    /**
     * Sets the start row.
     *
     * @param startRow the new start row
     */
    public void setStartRow(byte[] startRow) {
        this.startRow = startRow;
    }

    /**
     * Sets the end row.
     *
     * @param endRow the new end row
     */
    public void setEndRow(byte[] endRow) {
        this.endRow = endRow;
    }

    /**
     * Sets the htable.
     *
     * @param htable the new htable
     */
    public void setHtable(HTable htable) {
        this.htable = htable;
    }

    /**
     * Sets the scan row cache size.
     *
     * @param scanRowCacheSize the new scan row cache size
     */
    public void setScanRowCacheSize(int scanRowCacheSize) {
        this.scanRowCacheSize = scanRowCacheSize;
    }

    /**
     * Sets the column delimiter.
     *
     * @param columnDelimiter the new column delimiter
     */
    public void setColumnDelimiter(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        if (this.scanner != null)
            this.scanner.close();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    @Override
    public Text createKey() {
        return new Text();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    @Override
    public Text createValue() {
        return new Text();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#getPos()
     */
    @Override
    public long getPos() throws IOException {
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#getProgress()
     */
    @Override
    public float getProgress() throws IOException {
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object, java.lang.Object)
     */
    @Override
    public boolean next(Text key, Text value) throws IOException {
        Result result = this.scanner.next();

        if (result != null) {
            key.set(result.getRow());
            StringBuilder tempValue = new StringBuilder();
            
            for (int i = 0; i < inputColumnDescriptors.size(); i++) {
                HbaseColumnDescriptor hcd = inputColumnDescriptors.get(i);
                    
                // TODO: make this work for non-string pentaho types
                byte[] colVal = result.getValue(hcd.getFamily(), hcd.getQualifier());

                if (i > 0)
                    tempValue.append(this.columnDelimiter);

                if (colVal != null)
                    tempValue.append(Bytes.toString(colVal));
            }

            value.set(tempValue.toString());

            return true;
        }

        return false;
    }
}
