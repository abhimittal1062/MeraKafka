package com.merakafka;

import com.merakafka.storage.TopicManager;

import java.io.IOException;
import java.nio.file.Path;

public class MeraKafkaApplication {
    public static void main(String[] args) throws IOException {
        TopicManager manager = new TopicManager(Path.of("data", "topics"));

        System.out.println("Mera-Kafka Has Started!!");
        System.out.println("Thanks for using Mera-Kafka!!");
        if (manager.size("orders") == 0) {
            manager.append("orders", new Message("orders", "First Order"));
            manager.append("orders", new Message("orders", "Second Order"));
            manager.append("orders", new Message("orders", "Third Order"));
        }

        if (manager.size("payments") == 0) {
            manager.append("payments", new Message("payments", "First Payment"));
            manager.append("payments", new Message("payments", "Second Payment"));
            manager.append("payments", new Message("payments", "Third Payment"));
        }

        System.out.println("\nOrders topic:");
        for (int i = 0; i < manager.size("orders"); i++) {
            System.out.println(manager.read("orders", i));
        }

        System.out.println("\nPayments topic:");
        for (int i = 0; i < manager.size("payments"); i++) {
            System.out.println(manager.read("payments", i));
        }
    }
}