package com.intersysconsulting.spark.storageformats.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;

/**
 * An example class that writes out a parquet file and then reads it back in.
 *
 * @author Victor M. Miller
 */
public class ParquetReadWriteExample {

    private MessageType schema = MessageTypeParser.parseMessageType(
            "message Pair {\n" +
            " required binary left (UTF8);\n" +
            " required binary right (UTF8);\n" +
            "}");

    private Path path = new Path("data.parquet");

    public static void main(String[] args) throws Exception {
        ParquetReadWriteExample example = new ParquetReadWriteExample();
        example.write();
        example.read();
    }

    private void write() throws Exception {

        GroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup()
                .append("left", "L")
                .append("right", "R");

        ParquetWriter<Group> writer = new SimpleParquetWriterBuilder(path)
                .withSchema(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();

        writer.write(group);
        writer.close();
    }

    private void read() throws Exception {
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        ParquetReader<Group> reader = ParquetReader.builder(groupReadSupport, path).build();
        Group group = reader.read();

        System.out.println("Left: " + group.getString("left", 0));
        System.out.println("Right: " + group.getString("right", 0));

        File file = new File("data.parquet");
        if (file.exists()) {
            file.delete();
            new File(".data.parquet.crc").delete();
        }
    }
}
