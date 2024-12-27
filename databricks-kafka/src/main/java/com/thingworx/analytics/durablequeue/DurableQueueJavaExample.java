package com.thingworx.analytics.durablequeue;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/*** SSL configurations
 * Kafka server configs
 *
 * listeners=SSL://<host>:<port>
 * ssl.client.auth=required
 * ssl.keystore.location=/opt/kafka/server.keystore.jks
 * ssl.keystore.password=password
 * ssl.key.password=password
 * ssl.truststore.location=/opt/kafka/truststore
 * ssl.truststore.password=password
 * inter.broker.listener.name=SSL
 *
 *
 * Client configs
 *
 * security.protocol=SSL
 * ssl.truststore.location=/opt/kafka/truststore
 * ssl.truststore.password=password
 * ssl.keystore.location=/opt/kafka/client.keystore.jks
 * ssl.keystore.password=password
 * ssl.key.password=password
 */

/*
 Args options for the main method
     kafka.host.name: kafka server ip or hostname
     kafka.host.port: kafka server listener port
     kafka.queue.topic: kafka server topic

     ssl.security.enabled: set to true to enable SSL
     ssl.keystore.location: client keystore location on dbfs i.e. "/dbfs/FileStore/shared_uploads/user/client_keystore.jks"
     ssl.keystore.password: password for client keystore i.e. "password"
     ssl.truststore.location: client truststore location on dbfs i.e. "/dbfs/FileStore/shared_uploads/user/truststore"
     ssl.truststore.password: password for client truststore i.e. "password"

     kafka.user.name: username. This can be use if no ssl configured.
     kafka.user.pass: password of the kafka user

     Here is an example of the parameter for databricks jar job:
     ["kafka.host.name:ip", "kafka.host.port:port", "kafka.queue.topic:durable-queue", "ssl.security.enabled:true", "ssl.keystore
     .location:/dbfs/FileStore/shared_uploads/user/client_keystore.jks", "ssl.keystore.password:password", "ssl.truststore
     .location:/dbfs/FileStore/shared_uploads/user/truststore", "ssl.truststore.password:password"]
 */
public class DurableQueueJavaExample {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("Parameters can't be null or empty");
        }

        Map<String,String> options = getMapFromArgs(args);
        SparkSession spark = SparkSession.builder().getOrCreate();

        String host = options.get("kafka.host.name");
        String port = options.get("kafka.host.port");
        String queueName = options.get("kafka.queue.topic");
        boolean isSecure = Optional.ofNullable(options.get("ssl.security.enabled")).orElse("false").equalsIgnoreCase("true");

        DataStreamReader reader =
            spark.readStream().format("kafka").option("kafka.bootstrap.servers", host + ":" + port).option("subscribe", queueName);

        Dataset<Row> valueDF = getRowDataset(isSecure, reader, options);

        StreamingQuery query = valueDF.writeStream().outputMode("append").format("console").start();

        query.awaitTermination();
    }

    private static Dataset<Row> getRowDataset(boolean isSecure, DataStreamReader reader, Map<String,String> options) {
        if (isSecure) {
            String keyPath = options.get("ssl.keystore.location");
            String KeyPass = options.get("ssl.keystore.password");
            String trustPath = options.get("ssl.truststore.location");
            String trustPass = options.get("ssl.truststore.password");

            reader.option("kafka.security.protocol", "SSL")
                .option("kafka.ssl.client.auth", "required") // This enables the client cert auth on server side
                .option("kafka.ssl.keystore.location", keyPath)
                .option("kafka.ssl.keystore.password", KeyPass)
                .option("kafka.ssl.truststore.location", trustPath)
                .option("kafka.ssl.truststore.password", trustPass);
        } else {
            reader.option("kafka.security.protocol", "PLAINTEXT")
                // Below 2 lines of options can be removed if kafka server is running as plaintext and no auth configured.
                .option("kafka.security.protocol", "SASL_PLAINTEXT").option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + options.get("kafka.user.name") + "\"|password" +
                        "=\"" + options.get("kafka.user.pass") + "\"");
        }

        Dataset<Row> df = reader.load();
        Dataset<Row> valueDF = df.selectExpr("CAST(value AS STRING)");
        return valueDF;
    }

    private static Map<String,String> getMapFromArgs(String[] args) {
        Map<String,String> map = new HashMap();
        Arrays.stream(args).iterator().forEachRemaining(s -> {
            System.out.println(s);
            String[] pair = s.split(":");
            if (pair.length != 2) {
                throw new IllegalArgumentException("Argument [" + s + "] doesn't follow the key:value format.");
            }
            map.put(pair[0], pair[1]);
        });

        return map;
    }
}
