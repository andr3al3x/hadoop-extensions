package org.pentaho.hadoop.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyRecordWriter;
import org.apache.avro.mapreduce.AvroOutputFormatBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class JsonToAvroOutputFormat extends AvroOutputFormatBase<Text, NullWritable>  {

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
	    Configuration conf = job.getConfiguration();
	    // Get the writer schema.
	    Schema writerSchema = AvroJob.getOutputKeySchema(conf);
	    boolean isMapOnly = job.getNumReduceTasks() == 0;
	    if (isMapOnly) {
	      Schema mapOutputSchema = AvroJob.getMapOutputKeySchema(conf);
	      if (mapOutputSchema != null) {
	        writerSchema = mapOutputSchema;
	      }
	    }
	    if (null == writerSchema) {
	      throw new IOException(
	          "AvroKeyOutputFormat requires an output schema. Use AvroJob.setOutputKeySchema().");
	    }
	    
	    GenericData dataModel = AvroSerialization.createDataModel(conf);

	    return new PentahoAvroWrapperRecordWriter(writerSchema, dataModel, getCompressionCodec(job),
	       getAvroFileOutputStream(job), getSyncInterval(job));
	}
	
	
	class PentahoAvroWrapperRecordWriter extends RecordWriter<Text, NullWritable>{
		private AvroKeyRecordWriter<Record> avroWriter;
		DatumReader<Schema> reader;
		Schema schema;
		
		public PentahoAvroWrapperRecordWriter(Schema writerSchema, GenericData dataModel, CodecFactory compressionCodec,
		        OutputStream outputStream, int syncInterval) throws IOException {
			avroWriter= new AvroKeyRecordWriter<Record>(writerSchema, dataModel, compressionCodec, outputStream, syncInterval);
		}
		
		@Override
		public void write(Text key, NullWritable ignore) throws IOException, InterruptedException {
			String			json 		= key.toString();
			InputStream		input 		= new ByteArrayInputStream(json.getBytes("UTF-8"));
			reader 						= new GenericDatumReader<Schema>(schema);
			Decoder			decoder 	= DecoderFactory.get().jsonDecoder(schema, input);
			GenericRecord	datum 		= (GenericRecord) reader.read(schema, decoder);

			this.avroWriter.write(new AvroKey<GenericData.Record>((Record) datum), ignore);			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			avroWriter.close(context);
			
	}
		
		
		
	}

}
