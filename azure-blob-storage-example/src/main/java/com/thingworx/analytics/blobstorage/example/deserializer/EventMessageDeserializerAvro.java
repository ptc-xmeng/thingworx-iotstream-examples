package com.thingworx.analytics.blobstorage.example.deserializer;

import com.thingworx.analytics.blobstorage.example.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.function.Function;

public class EventMessageDeserializerAvro implements Function<byte[], AbstractMap.SimpleEntry> {

    private DatumReader<GenericRecord> reader;

    private Schema schema;

    public EventMessageDeserializerAvro(Schema schema) {
        reader = new GenericDatumReader<>(schema);
        this.schema = schema;
    }

    @Override
    public AbstractMap.SimpleEntry<Message, GenericRecord> apply(byte[] data) {
        Message message = new Message();
        GenericRecord record = null;
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new String(data));
            record = reader.read(null, decoder);
            Class<?> clazz = message.getClass();
            for (Field f : clazz.getDeclaredFields()) {
                if (clazz.getDeclaredField(f.getName()).getType().equals(String.class)) {
                    clazz.getDeclaredField(f.getName()).set(message, record.get(f.getName()).toString());
                } else {
                    clazz.getDeclaredField(f.getName()).set(message, record.get(f.getName()));
                }
            }
        } catch (IOException | NoSuchFieldException | IllegalAccessException e) {
            return null;
        }

        return new AbstractMap.SimpleEntry<>(message, record);
    }

}
