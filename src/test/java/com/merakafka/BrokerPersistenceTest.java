
package com.merakafka;

import com.merakafka.client.KafkaConsumer;
import com.merakafka.Message;
import java.util.List;

public class BrokerPersistenceTest {

    public static void main(String[] args) throws Exception {
        String topic = "demo";
        long expectedOffset = 13;
        String expectedValue = "Hello from Producer and Consumer!";

        KafkaConsumer consumer =
                new KafkaConsumer("localhost", 9092);

        List<Message> messages =
                consumer.poll(topic, expectedOffset, 1024 * 1024);

        for (Message message : messages) {
            System.out.println(
                    "FETCHED: offset=" + message.getOffset()
                            + ", value=" + message.getValue()
            );

            if (message.getOffset() == expectedOffset
                    && expectedValue.equals(message.getValue())) {
                System.out.println(
                        "PASS: message persisted after broker restart"
                );
                return;
            }
        }

        throw new AssertionError(
                "Persisted message not found at offset " + expectedOffset
        );
    }
}
