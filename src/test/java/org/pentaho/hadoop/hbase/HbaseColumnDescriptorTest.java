package org.pentaho.hadoop.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.pentaho.hadoop.mapred.hbase.HbaseColumnDescriptor;
import org.pentaho.hadoop.mapred.hbase.HbaseColumnDescriptor.HbaseColumnType;

import junit.framework.TestCase;

public class HbaseColumnDescriptorTest extends TestCase {

    public void testHbaseColumnDescriptorNumber() {
        String testDescriptorNumber = "pageviews:jan:number";
        
        HbaseColumnDescriptor hcd = new HbaseColumnDescriptor(testDescriptorNumber);
        
        assertEquals("pageviews", Bytes.toString(hcd.getFamily()));
        assertEquals("jan", Bytes.toString(hcd.getQualifier()));
        assertEquals(HbaseColumnType.NUMBER, hcd.getColumnType());
    }

    public void testHbaseColumnDescriptorInteger() {       
        String testDescriptorInteger = "pageviews:dec:integer";
        
        HbaseColumnDescriptor hcd = new HbaseColumnDescriptor(testDescriptorInteger);
        
        assertEquals("pageviews", Bytes.toString(hcd.getFamily()));
        assertEquals("dec", Bytes.toString(hcd.getQualifier()));
        assertEquals(HbaseColumnType.INTEGER, hcd.getColumnType());
    }
    
    public void testHbaseColumnDescriptorString() {       
        String testDescriptorString = "pageviews:jan:string";
        
        HbaseColumnDescriptor hcd = new HbaseColumnDescriptor(testDescriptorString);
        
        assertEquals("pageviews", Bytes.toString(hcd.getFamily()));
        assertEquals("jan", Bytes.toString(hcd.getQualifier()));
        assertEquals(HbaseColumnType.STRING, hcd.getColumnType());
    }
    
    public void testIsValid() {
        String validTestDescriptor = "pageviews:jan:number";
        String invalidTestDescriptor = "pageviews;jan;number";
        
        HbaseColumnDescriptor validHcd = new HbaseColumnDescriptor(validTestDescriptor);
        HbaseColumnDescriptor invalidHcd = new HbaseColumnDescriptor(invalidTestDescriptor);
        
        assertTrue(validHcd.isValid());
        assertFalse(invalidHcd.isValid());
    }

}
