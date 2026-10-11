package com.merakafka.storage;

import com.merakafka.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Partition {
    private final int partitionId;
    private final String topic;
    private final MessageLog messageLog;

    public Partition(String topic, int partitionId, Path dataDirectory) throws IOException {
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition ID cant be negative");
        }

        this.topic = topic;
        this.partitionId = partitionId;
        Path logFile = dataDirectory.resolve(topic).resolve("partition-" + partitionId).resolve("message.log");
        Files.createDirectories(logFile.getParent());
        this.messageLog = new MessageLog(logFile);
    }

    public long append(Message message) throws IOException {
        if (!topic.equals(message.getTopic())) {
            throw new IllegalArgumentException("Message Topic mismatch");
        }

        return messageLog.append(message);
    }

    public Message read(long offset) throws IOException {
        return messageLog.read(offset);
    }

    public int size() {
        return messageLog.size();
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getTopic() {
        return topic;
    }
}
