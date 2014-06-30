package org.pentaho.hadoop.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * Outputs multiple text files based on the key path
 */
public class KeyBasedMultipleTextOutputFormat extends MultipleTextOutputFormat<Text, Text> {

    /**
     * Overriden method that returns the file name based on the key value
     *
     * @param key
     * @param value
     * @param name
     * @return
     */
    @Override
    protected String generateFileNameForKeyValue(Text key, Text value, String name) {
        return new Path(key.toString(), name).toString();
    }


    /**
     * Overriden method used to discard the key as its being used in the path already
     *
     * @param key
     * @param value
     * @return
     */
    @Override
    protected Text generateActualKey(Text key, Text value) {
        return null;
    }
}
