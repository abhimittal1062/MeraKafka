package com.merakafka.client;

import java.io.IOException;

public class KafkaProducer {
    private final KafkaClient client;
    public KafkaProducer(String host,int port) {
        this.client=new KafkaClient(host, port);
    }

    public long send(String topic,String value) throws IOException{
        return client.produce(topic, value);
    }
}
