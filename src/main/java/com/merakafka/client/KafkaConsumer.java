package com.merakafka.client;

import com.merakafka.Message;

import java.io.IOException;
import java.util.List;

public class KafkaConsumer {
    private final KafkaClient client;

    public KafkaConsumer(String host,int port) {
        this.client=new KafkaClient(host,port);
    }

    public List<Message> poll(String topic,long offset,int maxBytes) throws IOException {
        return client.fetch(topic,offset,maxBytes);
    }
}