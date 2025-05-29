package com.thingworx.analytics.blobstorage.example;

import com.thingworx.analytics.blobstorage.example.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.ReflectDataSupplier;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
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

    public void writeBulk(String path, Schema schema, Collection<Message> records) throws IOException {
        String newPath = pathTemplate + path;
        try (ParquetWriter<Message> parquetWriter =
                 AvroParquetWriter.<Message>builder(new Path(newPath))
                     .withConf(conf)
                     .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                     .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                     .withSchema(schema)
                     .build()) {
            for (Message record : records){
                parquetWriter.write(record);
            }
        }
    }

    public void readFiles(String path) throws IOException {
        conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
        AvroReadSupport.setAvroDataSupplier(conf, ReflectDataSupplier.class);

        try (ParquetReader<Message> parquetReader = AvroParquetReader.<Message>builder(new Path(path)).withConf(conf).build()) {
            Message record = parquetReader.read();
            while (record != null) {
                System.out.println(record.toString());
                record = parquetReader.read();
            }
        }
    }
}
