package com.thingworx.analytics.blobstorage.example.message;

public class Message {

    public int value;
    public String name;
    public String source;
    public long timestamp;
    public String quality;

    @Override
    public String toString() {
        return "Message{" + "value=" + value + ", name='" + name + '\'' + ", source='" + source + '\'' + ", timestamp=" + timestamp +
            ", quality='" + quality + '\'' + '}';
    }
}
