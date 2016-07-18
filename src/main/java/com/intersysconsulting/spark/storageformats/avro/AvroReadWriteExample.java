package com.intersysconsulting.spark.storageformats.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * An example class that writes Avro to a byte buffer and then reads it using the Generic API for Avro.
 *
 * @author Victor M. Miller
 */
public class AvroReadWriteExample {

    private Schema schema = null;

    public static void main(String[] args) throws Exception {
        AvroReadWriteExample example = new AvroReadWriteExample();
        ByteArrayOutputStream out = example.write();
        example.read(out);
    }

    private ByteArrayOutputStream write() throws Exception {
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(getClass().getResourceAsStream("/StringPair.avsc"));

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

        return out;
    }

    private void read(ByteArrayOutputStream out) throws Exception {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

        GenericRecord record = reader.read(null, decoder);
        System.out.println("Left: " + record.get("left").toString());
        System.out.println("Right: " + record.get("right").toString());
    }
}
