package org.pentaho.hadoop.mapred.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

// TODO: Auto-generated Javadoc
/**
 * The Class HbaseFlatTableInputFormat.
 */
@SuppressWarnings("deprecation")
public class HbaseFlatTableInputFormat implements InputFormat<Text, Text>, JobConfigurable {

    /** The log. */
    private final Log log = LogFactory.getLog(HbaseFlatTableInputFormat.class);

    /** The name of the table to read from. */
    public static final String SCAN_INPUT_TABLE = "hbase.mapred.inputtable";

    /** The number of rows (integer) for caching that will be passed to scanners. */
    public static final String SCAN_CACHEDROWS = "hbase.mapred.scan.cachedrows";

    /** The timestamp (long) used to filter columns with a specific timestamp. */
    public static final String SCAN_TIMESTAMP = "hbase.mapred.scan.timestamp";

    /** The starting timestamp (long) used to filter columns with a specific range of versions. */
    public static final String SCAN_TIMERANGE_START = "hbase.mapred.scan.timerange.start";

    /** The ending timestamp (long) used to filter columns with a specific range of versions. */
    public static final String SCAN_TIMERANGE_END = "hbase.mapred.scan.timerange.end";

    /** The Constant COLUMN_LIST. */
    public static final String SCAN_COLUMN_LIST = "hbase.mapred.tablecolumns";

    /** The Constant SCAN_COLUMN_DELIMITER. */
    public static final String SCAN_COLUMN_DELIMITER = "hbase.mapred.output.valuedelimiter";
    
    /** The input column descriptors. */
    private List<HbaseColumnDescriptor> inputColumnDescriptors;

    /** The htable. */
    private HTable htable;
  
    /**
     * Instantiates a new hbase flat table input format.
     */
    public HbaseFlatTableInputFormat() {
        this.inputColumnDescriptors = new ArrayList<HbaseColumnDescriptor>();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.JobConfigurable#configure(org.apache.hadoop.mapred.JobConf)
     */
    @Override
    public void configure(JobConf job) {
        String tableName = job.get(SCAN_INPUT_TABLE);
        String colArg = job.get(SCAN_COLUMN_LIST);

        if (!StringUtils.isBlank(colArg)) {
            String[] colNames = colArg.split(" ");
            
            for (int i = 0; i < colNames.length; i++) {
                HbaseColumnDescriptor hcd = new HbaseColumnDescriptor(colNames[i]);
                
                if(!hcd.isValid())
                    throw new RuntimeException("invalid column descriptor: " + colNames[i]);
                
                inputColumnDescriptors.add(hcd);
            }
        }

        try {
            this.htable = new HTable(HBaseConfiguration.create(job), tableName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit,
     * org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.Reporter)
     */
    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit inputSplit, JobConf job, Reporter reporter)
            throws IOException {

        TableSplit tableSplit = (TableSplit) inputSplit;
        
        int scanRowsCacheSize = Integer.parseInt(StringUtils.defaultIfBlank(job.get(SCAN_CACHEDROWS),"100"));
        String columnDelimiter = StringUtils.defaultIfBlank(job.get(SCAN_COLUMN_DELIMITER), ";");
        
        Long timestamp = null;
        String timestampStr = job.get(SCAN_TIMESTAMP);
        if(!StringUtils.isBlank(timestampStr))
            timestamp = new Long(timestampStr);
        
        HbaseFlatTableRecordReader recordReader = new HbaseFlatTableRecordReader();
        recordReader.setColumnDelimiter(columnDelimiter);
        recordReader.setStartRow(tableSplit.getStartRow());
        recordReader.setEndRow(tableSplit.getEndRow());
        recordReader.setInputColumnDescriptors(this.inputColumnDescriptors);
        recordReader.setScanRowCacheSize(scanRowsCacheSize);
        recordReader.setTimestamp(timestamp);
        recordReader.setHtable(this.htable);        
        recordReader.init();
        
        return recordReader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf, int)
     */
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        if (this.htable == null) {
            throw new IOException("No table was provided");
        }

        byte[][] startKeys = this.htable.getStartKeys();
        if (startKeys == null || startKeys.length == 0) {
            throw new IOException("Expecting at least one region");
        }

        if (this.inputColumnDescriptors.isEmpty()) {
            throw new IOException("Expecting at least one column");
        }

        int realNumSplits = numSplits > startKeys.length ? startKeys.length : numSplits;
        InputSplit[] splits = new InputSplit[realNumSplits];
        int middle = startKeys.length / realNumSplits;
        int startPos = 0;
        for (int i = 0; i < realNumSplits; i++) {
            int lastPos = startPos + middle;
            lastPos = startKeys.length % realNumSplits > i ? lastPos + 1 : lastPos;
            String regionLocation = htable.getRegionLocation(startKeys[startPos], false).getHostname();

            splits[i] = new TableSplit(this.htable.getTableName(), startKeys[startPos],
                    ((i + 1) < realNumSplits) ? startKeys[lastPos] : HConstants.EMPTY_START_ROW, regionLocation);

            log.info("split: " + i + "->" + splits[i]);

            startPos = lastPos;
        }
        return splits;
    }
}
