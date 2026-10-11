package com.merakafka.storage;

import com.merakafka.Message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicManager {
    private final Path baseDirectory;
    private final ConcurrentMap<String, MessageLog> logs = new ConcurrentHashMap<>();

    public TopicManager(Path baseDirectory) throws IOException {
        this.baseDirectory = baseDirectory.toAbsolutePath().normalize();
        Files.createDirectories(this.baseDirectory);

        //Load existing topics from the base directory
        try(var paths=Files.list(this.baseDirectory)) {
            for(Path path:paths.filter(Files::isDirectory).toList()) {
                if(Files.exists(path.resolve("message.log"))) {
                    validateTopic(path.getFileName().toString());
                    getOrCreateLog(path.getFileName().toString());
                }
            }
        }
    }

    private void validateTopic(String topic) {
        if (topic == null || !topic.matches("[a-zA-Z0-9._-]+") || topic.equals(".") || topic.equals("..")) {
            throw new IllegalArgumentException("Invalid topic name");
        }
    }

    private MessageLog getOrCreateLog(String topic) throws IOException {
        validateTopic(topic);

        MessageLog existingLog = logs.get(topic);
        if (existingLog != null) {
            return existingLog;
        }

        synchronized(logs) {
            existingLog=logs.get(topic);
            if(existingLog!=null) {
                return existingLog;
            }
            Path logPath = baseDirectory.resolve(topic).resolve("message.log");
            MessageLog newLog = new MessageLog(logPath);

            logs.put(topic, newLog);
            return newLog;
        }
    }

    public MessageLog getExistingLog(String topic) throws IOException{
        validateTopic(topic);
        MessageLog log=logs.get(topic);

        if(log==null) {
            throw new IllegalArgumentException("Unknown topic: "+topic);
        }
        return log;
    }

    public long append(String topic, Message message) throws IOException {
        if (message==null || !topic.equals(message.getTopic())) {
            throw new IllegalArgumentException("Topic mismatch");
        }

        MessageLog log = getOrCreateLog(topic);
        return log.append(message);
    }

    public Message read(String topic, long offset) throws IOException {
        if(offset<0) {
            throw new IllegalArgumentException("Offset cant be negative");
        }
        MessageLog log = getOrCreateLog(topic);
        return log.read(offset);
    }

    public int size(String topic) throws IOException {
        MessageLog log = getOrCreateLog(topic);
        return log.size();
    }

    public void createTopic(String topic) throws IOException {
        validateTopic(topic);

        synchronized(logs) {
            validateTopic(topic);

            Path topicDirectory = baseDirectory.resolve(topic);

            if (logs.containsKey(topic) || Files.exists(topicDirectory)) {
                throw new IllegalArgumentException("Topic Already Exists: " + topic);
            }

            getOrCreateLog(topic);
        }
    }

    public java.util.List<String> getTopicNames() {
        return logs.keySet().stream().sorted().toList();
    }
}