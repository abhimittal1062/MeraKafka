package com.merakafka.zookeeper;

import com.merakafka.broker.BrokerInfo;

public class ZooKeeperTest {
    public static void main(String[] args) throws Exception {
        try (ZooKeeperClient client=new ZooKeeperClient("localhost:2181",5000)) {
            System.out.println("Connected to Zoo Keeper");
            BrokerInfo broker=new BrokerInfo(1,"localhost",9092);
            client.registerBroker(broker);
            System.out.println("Registered Broker IDs: "+client.getRegisteredBrokers());
            System.out.println("Broker Data: "+client.getData("/merakafka/brokers/1"));
        }
    }
}
