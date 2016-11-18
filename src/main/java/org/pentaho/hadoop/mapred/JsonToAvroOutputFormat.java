package org.pentaho.hadoop.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;


public class JsonToAvroOutputFormat extends FileOutputFormat<Text, NullWritable>  {
	private AvroOutputFormat<Record> avro = new AvroOutputFormat<Record>();
	
	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
			throws FileAlreadyExistsException, InvalidJobConfException, IOException {
		avro.checkOutputSpecs(arg0, arg1);
	}

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(FileSystem ignore, JobConf job, String name, Progressable prog) throws IOException {
		boolean isMapOnly = job.getNumReduceTasks() == 0;
		Schema schema = isMapOnly 
			      ? AvroJob.getMapOutputSchema(job)
			      : AvroJob.getOutputSchema(job);
			      
		return new PentahoAvroWrapperRecordWritter(schema, avro.getRecordWriter(ignore, job, name, prog)); 
	}
	
	
	public class PentahoAvroWrapperRecordWritter implements RecordWriter<Text, NullWritable> {

		RecordWriter<AvroWrapper<Record>,NullWritable> wrappedWritter;
		DatumReader<Schema> reader;
		Schema schema;
		
		public PentahoAvroWrapperRecordWritter(Schema schema, RecordWriter<AvroWrapper<Record>,NullWritable> wrappedWritter) {
			this.wrappedWritter = wrappedWritter;
			this.schema = schema;
		}
		
		@Override
		public void close(Reporter reporter) throws IOException {
			this.wrappedWritter.close(reporter);
		}

		@Override
		public void write(Text jsonAsText, NullWritable nullArg) throws IOException {
			String			json 		= jsonAsText.toString();
			InputStream		input 		= new ByteArrayInputStream(json.getBytes("UTF-8"));
			reader 			= new GenericDatumReader<Schema>(schema);
			Decoder			decoder 	= DecoderFactory.get().jsonDecoder(schema, input);
			GenericRecord	datum 		= (GenericRecord) reader.read(schema, decoder);

			this.wrappedWritter.write(new AvroWrapper<Record>((Record) datum), nullArg);
		}
		
	}
	



}
