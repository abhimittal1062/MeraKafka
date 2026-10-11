package com.merakafka;

import com.merakafka.client.KafkaConsumer;
import com.merakafka.client.KafkaProducer;

import java.util.List;

public class ProducerConsumerTest {

    public static void main(String[] args) throws Exception {

        String host = "localhost";
        int port = 9092;
        String topic = "demo";

        KafkaProducer producer = new KafkaProducer(host, port);
        KafkaConsumer consumer = new KafkaConsumer(host, port);

        String value = "Hello from KafkaProducer and KafkaConsumer!";

        // 1. Produce message
        long offset = producer.send(topic, value);

        System.out.println("PRODUCED: offset=" + offset
                + ", value=" + value);

        // 2. Fetch the produced message
        List<Message> messages = consumer.poll(
                topic, offset, 1024 * 1024
        );

        // 3. Verify result
        boolean found = false;

        for (Message message : messages) {
            if (message.getOffset() == offset
                    && message.getValue().equals(value)) {

                found = true;
                System.out.println("CONSUMED: " + message);
                break;
            }
        }

        if (!found) {
            throw new AssertionError(
                    "Produced message was not found by consumer"
            );
        }

        System.out.println(
                "PASS: Producer and Consumer work correctly!"
        );
    }
}