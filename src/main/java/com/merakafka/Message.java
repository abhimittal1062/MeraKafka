package com.merakafka;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Message {
    private final String topic;
    private final String value;
    private final Instant timestamp;
    private long offset = -1;

    public Message(String topic, String value) {
        this(topic, value, Instant.now());
    }

    public Message(String topic, String value, Instant timestamp) {
        this.topic = topic;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public String getValue() {
        return value;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        if (this.offset != -1) {
            throw new IllegalStateException("Offset Already Assigned");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset Cannot be Negative");
        }
        this.offset = offset;
    }

    public String getReadableTimestamp() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")
                .withZone(ZoneId.of("Asia/Kolkata"));
        return formatter.format(timestamp);
    }

    @Override
    public String toString() {
        return "Message{" + "topic=\'" + topic + '\'' +
                ",value=\'" + value + '\'' + ",offset=\'" + offset + '\'' +
                ",timestamp=\'" + getReadableTimestamp() + '\'' + '}';
    }
}