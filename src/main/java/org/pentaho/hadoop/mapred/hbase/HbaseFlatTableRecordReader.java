package org.pentaho.hadoop.mapred.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
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
    
    /** The input columns. */
    private List<byte[][]> inputColumns;

    /**
     * Instantiates a new hbase flat table record reader.
     *
     * @param startRow the start row
     * @param endRow the end row
     * @param htable the htable
     * @param scanRowCacheSize the scan row cache size
     * @param inputColumns the input columns
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public HbaseFlatTableRecordReader(byte[] startRow, byte[] endRow, HTable htable, int scanRowCacheSize,
            byte[][] inputColumns) throws IOException {
        this.inputColumns = new ArrayList<byte[][]>();

        Scan scan = new Scan(startRow, endRow);
        scan.setCacheBlocks(false);
        scan.setCaching(scanRowCacheSize);

        configureScanWithInputColumns(scan, inputColumns);

        this.scanner = htable.getScanner(scan);
    }

    /**
     * Configure scan with input columns.
     *
     * @param scan the scan
     * @param inputColumns the input columns
     */
    protected void configureScanWithInputColumns(Scan scan, byte[][] inputColumns) {
        for (byte[] familyAndQualifier : inputColumns) {
            byte[][] fq = KeyValue.parseColumn(familyAndQualifier);
            this.inputColumns.add(fq);

            if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
                scan.addColumn(fq[0], fq[1]);
            } else {
                scan.addFamily(fq[0]);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        if(this.scanner != null)
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

            for (byte[][] fq : inputColumns) {
                byte[] colVal = result.getValue(fq[0], fq[1]);
                
                if (tempValue.length() != 0)
                    tempValue.append("|");
                
                if (colVal != null)
                    tempValue.append(Bytes.toString(colVal));           
            }

            value.set(tempValue.toString());

            return true;
        }

        return false;
    }
}
