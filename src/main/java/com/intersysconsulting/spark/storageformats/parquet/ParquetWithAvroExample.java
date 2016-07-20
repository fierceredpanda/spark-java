package com.intersysconsulting.spark.storageformats.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.File;

/**
 * An example of writing and reading Parquet files using the Avro API.
 *
 * @author Victor M. Miller
 */
public class ParquetWithAvroExample {

    private Path path = new Path("data_from_avro.parquet");

    public static void main(String[] args) throws Exception {
        ParquetWithAvroExample example = new ParquetWithAvroExample();
        example.write();
        example.read();
    }

    private void write() throws Exception {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("/avro/StringPair.avsc"));

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();

        writer.write(datum);
        writer.close();
    }

    private void read() throws Exception {

        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path).build();
        GenericRecord result = reader.read();

        System.out.println("Left: " + result.get("left").toString());
        System.out.println("Right: " + result.get("right").toString());

        File file = new File("data_from_avro.parquet");
        if (file.exists()) {
            file.delete();
            new File(".data_from_avro.parquet.crc").delete();
        }
    }
}
