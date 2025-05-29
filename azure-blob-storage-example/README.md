# What is this?
This is an example of using durable queue in TWX to persist data stream to Azure blob storage

# How to use it?
You need TWX, Kafka server and Azure blob storage account configured in order to run this example.
1. Update necessary information in `azure-blob-storage-example/src/main/java/com/thingworx/analytics/blobstorage/example
/AzureBlobKafkaStoragePersistence.java`  
<b>Necessary Fields</b>:
    ```
    public static final String QUEUE_NAME = "<topic name>";
    private static String accountName = "<storage account>";
    private static String accessKey = "<storage account key>";
    private static String containerName = "<container name>";
    public static final String KAFKA_HOST = "<kafka host>";
    ```
2. Run `mvn clean package` to build the executable jar  
3. Run the jar file by executing `java -jar \<jar name\>.jar`  

Notice that in this example by default kafka client is not SSL enabled. You can enable it by updating necessary configurations.
```
props.put("security.protocol", "SASL_SSL");
props.put("ssl.truststore.location", "/path/to/your/truststore.jks"); 
props.put("ssl.truststore.password", "your_truststore_password"); 
props.put("ssl.keystore.location", "/path/to/your/keystore.jks");
props.put("ssl.keystore.password", "your_keystore_password");
props.put("ssl.key.password", "your_key_password");
```

Check on the TWX help center <Setting Up and Integrating ThingWorx, Apache Kafka, and Azure Blob Storage> section for more details.  