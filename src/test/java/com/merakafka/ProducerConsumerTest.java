
package com.merakafka;

import com.merakafka.client.KafkaProducer;
import com.merakafka.client.KafkaConsumer;

import java.util.List;

public class ProducerConsumerTest {

    public static void main(String[] args) throws Exception {
        String topic = "demo";
        String value = "Hello from Producer and Consumer!";

        KafkaProducer producer =
                new KafkaProducer("localhost", 9092);

        KafkaConsumer consumer =
                new KafkaConsumer("localhost", 9092);

        // Produce a message.
        long offset = producer.send(topic, value);

        System.out.println(
                "PRODUCED: offset=" + offset + ", value=" + value
        );

        // Fetch the message starting at its offset.
        List<Message> messages =
                consumer.poll(topic, offset, 1024 * 1024);

        boolean found = false;

        for (Message message : messages) {
            System.out.println(
                    "CONSUMED: offset=" + message.getOffset()
                            + ", value=" + message.getValue()
            );

            if (message.getOffset() == offset
                    && value.equals(message.getValue())) {
                found = true;
            }
        }

        if (!found) {
            throw new AssertionError(
                    "Consumer could not retrieve the produced message"
            );
        }

        System.out.println("PASS: producer-to-consumer flow works");
    }
}
