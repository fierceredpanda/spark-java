package com.intersysconsulting.spark.storageformats.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple builder for the parquet writer.
 *
 * @author Victor M. Miller
 */
class SimpleParquetWriterBuilder extends ParquetWriter.Builder<Group, SimpleParquetWriterBuilder> {

    private MessageType schema = null;

    SimpleParquetWriterBuilder(Path path) {
        super(path);
    }

    SimpleParquetWriterBuilder withSchema(MessageType schema) {
        this.schema = schema;
        return this;
    }

    @Override
    protected SimpleParquetWriterBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<Group> getWriteSupport(Configuration conf) {
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, conf);
        return writeSupport;
    }

}
