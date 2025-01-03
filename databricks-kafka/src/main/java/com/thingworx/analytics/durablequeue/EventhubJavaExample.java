package com.thingworx.analytics.durablequeue;

import org.apache.spark.eventhubs.ConnectionStringBuilder;
import org.apache.spark.eventhubs.EventHubsConf$;
import org.apache.spark.eventhubs.EventHubsConf;
import org.apache.spark.eventhubs.EventPosition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EventhubJavaExample {
    public static final String ENDPOINT = "Endpoint=sb://<event hub>.servicebus.windows.net/;SharedAccessKeyName=test-policy;" +
        "SharedAccessKey=<key>";
    public static final String EVENTHUB = "durable-queue";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
            .appName("EventHubStreamExample")
            .getOrCreate();

        String connectionString = ConnectionStringBuilder
            .apply(ENDPOINT)
            .setEventHubName(EVENTHUB)
            .build();

        EventHubsConf eventHubsConf = EventHubsConf$.MODULE$.apply(connectionString)
            .setStartingPosition(EventPosition.fromEndOfStream());

        Dataset<Row> eventHubStream = spark.readStream()
            .format("eventhubs")
            .options(eventHubsConf.toMap())
            .load();

        Dataset<Row> messages = eventHubStream.selectExpr("CAST(body AS STRING)");

        messages.writeStream()
            .outputMode("append")
            .format("console")
            .start()
            .awaitTermination();
    }
}

