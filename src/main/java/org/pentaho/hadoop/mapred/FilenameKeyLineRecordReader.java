package org.pentaho.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Record Reader implementation that overrides the functionality of the original LineRecordReader to output the filename
 * as the key.
 */
public class FilenameKeyLineRecordReader implements RecordReader<Text, Text> {

    private LineRecordReader lineRecordReader;
    private String fileName;
    private LongWritable longKey;

    public FilenameKeyLineRecordReader(Configuration job,
                                       FileSplit split) throws IOException {
        lineRecordReader = new LineRecordReader(job, (FileSplit) split);
        fileName = split.getPath().getName();
        longKey = new LongWritable();
    }

    @Override
    public boolean next(Text key, Text value) throws IOException {

        // get the next line
        if (!lineRecordReader.next(longKey, value)) {
            return false;
        }

        // set the new key value with the filename
        key.set(fileName + '-' + longKey.toString());
        value.set(value);

        return true;
    }

    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return lineRecordReader.getPos();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }

    @Override
    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }
}
