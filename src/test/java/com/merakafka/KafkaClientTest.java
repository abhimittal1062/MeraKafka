
package com.merakafka;

import com.merakafka.client.KafkaClient;
import com.merakafka.Message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.io.IOException;

public class KafkaClientTest {

    public static void main(String[] args) throws Exception {
        KafkaClient client = new KafkaClient("localhost", 9092);

        String topic = "demo";
        String value = "Hello from KafkaClient!";

        long offset = client.produce(topic, value);

        System.out.println(
                "PRODUCE SUCCESS: offset=" + offset
        );

        java.util.List<Message> messages =
        client.fetch(topic, offset, 1024 * 1024);

        boolean found = false;

        for (Message message : messages) {
            System.out.println(
                    "FETCHED: offset=" + message.getOffset()
                            + ", value=" + message.getValue()
            );

            if (message.getOffset() == offset
                    && value.equals(message.getValue())) {
                found = true;
            }
        }

        if (!found) {
            throw new AssertionError("Produced message was not fetched");
        }

        System.out.println("PASS: KafkaClient produce/fetch works");
    }
}
