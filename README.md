# spark-java

This project contains sample Spark code written in Java.  There are also samples for working with some of the common
input types that Spark works with, Avro and Parquet.

### Parquet Examples 
###### (Based on Hadoop: The Definitive Guide, 4th Edition)

In the package com.intersysconsulting.spark.storageformats.parquet there are samples for working with
parquet files directly from java.

- ParquetReadWriteExample - Example of reading and writing using only the Parquet API.
- ParquetWithAvroExample - An example of reading and writing to Parquet using Avro.


### Avro Examples  
###### (Based on Hadoop: The Definitive Guide, 4th Edition)

In the package com.intersysconsulting.spark.storageformats.avro there are samples for working with 
the geneirc and the specific Avro API's.

- AvroReadWriteExample - Writes Generic Avro to a byte buffer and reads it back in.
- AvroSpecificReadWriteExample - Uses an Avro generated class to write to the byte buffer and read it back in.