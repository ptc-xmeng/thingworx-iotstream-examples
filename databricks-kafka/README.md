# What is this?
This contains example code that is able to run in databricks as executable jar.

# How to use it?
This example module has 2 main java classes that both could run as entry point in databricks.  This code build with spark 3.5.0 and 
scala 2.12, databricks needs to be on the same config.    
Update `<mainClass>com.thingworx.analytics.durablequeue.EventhubJavaExample</mainClass>` in pom to config which main you want to run.

### [EventhubJavaExample](src/main/java/com/thingworx/analytics/durablequeue/EventhubJavaExample.java)  
This is an example to show how to load data from eventhub into databricks.  
Update below value in the class:
```
public static final String ENDPOINT = "Endpoint=sb://<eventhub_name>.servicebus.windows.net/;SharedAccessKeyName=<key_name>;SharedAccessKey=<key>";
public static final String EVENTHUB = "<queue_name>";
```
Then run `maven clean package`, you will get a jar that is able to run in databricks.  

### [DurableQueueJavaExample](src/main/java/com/thingworx/analytics/durablequeue/DurableQueueJavaExample.java)  
This example create a jar file that load `args` from databricks job config and run code to demo how to load data from Kafka into 
databricks.
```
Here is an example of the parameter for databricks jar job:
 ["kafka.host.name:<ip>", "kafka.host.port:<port>", "kafka.queue.topic:<topic>", "ssl.security.enabled:true", "ssl.keystore.location:/dbfs/FileStore/shared_uploads/user/client_keystore.jks", "ssl.keystore.password:password", "ssl.truststore.location:/dbfs/FileStore/shared_uploads/user/truststore", "ssl.truststore.password:password"]
```
With above example you will be able to load data from kafka server at \<ip:port\> with \<topic\>. In this example the SSL for kafka is 
enabled. You can also disable it by updating the argument for the databricks jar job.