hadoop-extensions
=================

A project with random hadoop extensions


New JsontoAvroOutputFormats for Pentaho
--------------------------------------
On the PMR use "org.pentaho.hadoop.mapred.PentahoAvroOutputFormat" as
Output
define the schema as "avro.output.schema"
and in the MAPPER or REDUCER write String JSON on the KEY.

NOTE: The JSON needs to match the Schema defined, so Avro Decoder can
convert it
