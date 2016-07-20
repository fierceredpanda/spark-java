package com.intersysconsulting.spark.storageformats.avro;

import avro.generated.StringPair;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;

/**
 * Tests reading and writing Avro data from a Byte Output Stream using Specific Avro classes.
 *
 * @author Victor M. Miller
 */
public class AvroSpecificReadWriteExample {

    private Schema schema = null;

    public static void main(String[] args) throws Exception {
        AvroSpecificReadWriteExample example = new AvroSpecificReadWriteExample();
        ByteArrayOutputStream out = example.write();
        example.read(out);
    }

    private ByteArrayOutputStream write() throws Exception {
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(getClass().getResourceAsStream("/avro/StringPair.avsc"));

        StringPair pair = StringPair.newBuilder().setLeft("L").setRight("R").build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<StringPair> writer = new SpecificDatumWriter<>(StringPair.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(pair, encoder);
        encoder.flush();
        out.close();

        return out;
    }

    private void read(ByteArrayOutputStream out) throws Exception {
        DatumReader<StringPair> reader = new SpecificDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);

        StringPair pair = reader.read(null, decoder);
        System.out.println("Left: " + pair.getLeft());
        System.out.println("Right: " + pair.getRight());
    }
}
