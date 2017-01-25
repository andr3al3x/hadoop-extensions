package org.pentaho.hadoop.mapred;

/**
 * Created by carlos on 1/25/17.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapred.lib.CombineFileRecordReaderWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;

public class CombinedFilenameKeyTextInputFormat extends CombineFileInputFormat<Text,Text> {
    @SuppressWarnings({"rawtypes", "unchecked"})
    public RecordReader<Text, Text> getRecordReader(InputSplit split,
                                                            JobConf conf, Reporter reporter) throws IOException {
        return new CombineFileRecordReader(conf, (CombineFileSplit) split, reporter,
                FilenameKeyTextRecordReaderWrapper.class);
    }

    private static class FilenameKeyTextRecordReaderWrapper
            extends CombineFileRecordReaderWrapper<Text,Text> {
        // this constructor signature is required by CombineFileRecordReader
        public FilenameKeyTextRecordReaderWrapper(CombineFileSplit split, Configuration conf,
                                       Reporter reporter, Integer idx) throws IOException {
            super(new FilenameKeyTextInputFormat(), split, conf, reporter, idx);
        }
    }
}

