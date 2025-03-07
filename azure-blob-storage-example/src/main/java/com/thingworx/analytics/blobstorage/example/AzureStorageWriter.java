package com.thingworx.analytics.blobstorage.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Collection;

public class AzureStorageWriter {
    private String pathTemplate;
    private Configuration conf = new Configuration();

    public AzureStorageWriter(String accountName, String accessKey, String containerName) {
        this.conf.set(String.format("fs.azure.account.key.%s.blob.core.windows.net", accountName), accessKey);
        this.pathTemplate = String.format("wasbs://%s@%s.blob.core.windows.net/", containerName, accountName);
    }

    public void writeBulk(String path, Schema schema, Collection<GenericRecord> records) throws IOException {
        String newPath = pathTemplate + path;
        try (ParquetWriter<GenericRecord> parquetWriter =
                 AvroParquetWriter.<GenericRecord>builder(new Path(newPath))
                     .withConf(conf)
                     .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                     .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                     .withSchema(schema)
                     .build()) {
            for (GenericRecord record : records){
                parquetWriter.write(record);
            }
        }
    }
}
