package com.thingworx.analytics.blobstorage.example;

import com.thingworx.analytics.blobstorage.example.deserializer.EventMessageDeserializerAvro;
import com.thingworx.analytics.blobstorage.example.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Collections;
import java.util.Queue;

public class AzureBlobKafkaStoragePersistence {

    public static final String PATH = "messages";

    public static final String QUEUE_NAME = "<topic name>";

    private static String accountName = "<storage account>";
    private static String accessKey = "<storage account key>";
    private static String containerName = "<container name>";

    public static final String KAFKA_HOST = "<kafka host>";

    private static int BUFF_SIZE = 16;

    private static Queue<GenericRecord> queue = new LinkedList<>();

    public static void main(String[] args) {
        KafkaConsumer<String, byte[]> consumer = getStringKafkaConsumer();
        consumer.subscribe(Collections.singletonList(QUEUE_NAME));
        AzureStorageWriter writer = new AzureStorageWriter(accountName, accessKey, containerName);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        try {
            Schema.Parser parser = new Schema.Parser();
            final Schema schema = parser.parse(AzureBlobKafkaStoragePersistence.class.getClassLoader().getResourceAsStream("schema.avsc"));
            EventMessageDeserializerAvro deserializer = new EventMessageDeserializerAvro(schema);
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(5000);
                for (ConsumerRecord<String, byte[]> record : records) {
                    AbstractMap.SimpleEntry<Message, GenericRecord> msg = deserializer.apply(record.value());
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), msg.getKey().toString());
                    queue.add(msg.getValue());

                    if (queue.size() >= BUFF_SIZE) {
                        Collection<GenericRecord> col = new LinkedList<>();
                        while (!queue.isEmpty()) {
                            col.add(queue.poll());
                        }
                        writer.writeBulk(PATH + "/" + LocalDate.now().format(formatter), schema, col);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            consumer.close();
        }
    }

    private static KafkaConsumer<String, byte[]> getStringKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        //        props.put("schema.registry.url", "https://your_schema_registry:8081");
        props.put("specific.avro.reader", "true");
        props.put("security.protocol", "PLAINTEXT");
        //        props.put("ssl.truststore.location", "/path/to/your/truststore.jks");
        //        props.put("ssl.truststore.password", "your_truststore_password");
        //        props.put("ssl.keystore.location", "/path/to/your/keystore.jks");
        //        props.put("ssl.keystore.password", "your_keystore_password");
        //        props.put("ssl.key.password", "your_key_password");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        return consumer;
    }
}
