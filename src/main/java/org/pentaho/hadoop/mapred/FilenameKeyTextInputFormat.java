package org.pentaho.hadoop.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Input format that allows retrieving the filename via key
 */
public class FilenameKeyTextInputFormat extends FileInputFormat<Text, Text> implements JobConfigurable {

    private CompressionCodecFactory compressionCodecs = null;

    @Override
    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        if (null == codec) {
            return true;
        }
        return codec instanceof SplittableCompressionCodec;
    }

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit genericSplit, JobConf job,
                                                    Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        return new FilenameKeyLineRecordReader(job, (FileSplit) genericSplit);
    }
}
