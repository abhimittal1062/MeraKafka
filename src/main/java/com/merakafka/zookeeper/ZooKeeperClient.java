package com.merakafka.zookeeper;

import com.merakafka.broker.BrokerInfo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Collections;
import java.util.List;

public class ZooKeeperClient implements AutoCloseable {
    private final ZooKeeper zooKeeper;
    private static final String BROKERS_PATH="/merakafka/brokers";
    private static final String TOPICS_PATH="/merakafka/topics";

    public ZooKeeperClient(String connectString, int sessionTimeoutMs) throws IOException, InterruptedException {
        CountDownLatch connectedLatch = new CountDownLatch(1);

        zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedLatch.countDown();
            }
        });

        boolean connected = connectedLatch.await(sessionTimeoutMs, TimeUnit.MILLISECONDS);
        if (!connected) {
            zooKeeper.close();
            throw new IOException("Timed Out connecting to ZooKeeper: " + connectString);
        }
    }

    public void createPersistentNode(String path,String data) throws KeeperException, InterruptedException{
        byte[] bytes=data.getBytes(StandardCharsets.UTF_8);
        if(zooKeeper.exists(path, false)==null) {
            try {
                zooKeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            catch(KeeperException.NodeExistsException ignored) {
                //Another client created the node concurrently
            }
        }
        else {
            zooKeeper.setData(path, bytes, -1);
        }
    }

    public void createEphemeralNode(String path,String data) throws KeeperException, InterruptedException {
        zooKeeper.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void registerBroker(BrokerInfo broker) throws KeeperException, InterruptedException{
        createPersistentNode("/merakafka","");

        if(!nodeExists(BROKERS_PATH)) {
            try {
           zooKeeper.create(BROKERS_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            catch(KeeperException.NodeExistsException ignored) {
                //Parent node was created by another broker
            }
        }

        String brokerPath=BROKERS_PATH+"/"+broker.getBrokerId();
        String brokerData=broker.getHost()+":"+broker.getPort();

        // Broker registration is ephemeral: it disappears when this Zookeeper session expires
        zooKeeper.create(brokerPath, brokerData.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Registered broker "+broker.getBrokerId()+" at "+brokerData);
    }

    public List<String> getRegisteredBrokers() throws KeeperException, InterruptedException {
        if(!nodeExists(BROKERS_PATH)) {
            return Collections.emptyList();
        }

        List<String> brokerIds=zooKeeper.getChildren(BROKERS_PATH, false);

        return brokerIds;
    }

    public boolean nodeExists(String path) throws KeeperException, InterruptedException {
        return zooKeeper.exists(path, false)!=null;
    }

    public String getData(String path) throws KeeperException,InterruptedException{
        byte[] data=zooKeeper.getData(path,false,null);
        return data==null?"" : new String(data,StandardCharsets.UTF_8);
    }

    public void createTopicsPath() throws KeeperException, InterruptedException {
        createPersistentNode("/merakafka","");

        if(!nodeExists(TOPICS_PATH)) {
            try {
                zooKeeper.create(TOPICS_PATH,new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
            catch(KeeperException.NodeExistsException ignored) {
                //Another broker created the path concurrently
            }
        }
    }

    public void createTopicMetadata(String topic,String metadata) throws KeeperException,InterruptedException {
        createTopicsPath();
        String path=TOPICS_PATH+"/"+topic;

        zooKeeper.create(path, metadata.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public String getTopicMetadata(String topic) throws KeeperException,InterruptedException {
        String path=TOPICS_PATH+"/"+topic;

        if(!nodeExists(path)) {
            return null;
        }

        return getData(path);
    }

    public void updateTopicMetadata(String topic,String metadata) throws KeeperException,InterruptedException {
        String path=TOPICS_PATH+"/"+topic;
        zooKeeper.setData(path, metadata.getBytes(StandardCharsets.UTF_8), -1);
    }

    public boolean createTopicMetadataIfAbsent(String topic,String metadata) throws KeeperException, InterruptedException {
        if(topic==null || !topic.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("Invalid topic name");
        }

        createTopicsPath();

        String path=TOPICS_PATH+"/"+topic;

        try {
            zooKeeper.create(path,metadata.getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            return true;
        }
        catch(KeeperException.NodeExistsException e) {
            return false;
        }
    }

    public List<String> getTopicNames() throws KeeperException,InterruptedException {
        if(!nodeExists(TOPICS_PATH)) {
            return Collections.emptyList();
        }
        return zooKeeper.getChildren(TOPICS_PATH, false).stream().sorted().toList();
    }

    @Override 
    public void close() throws InterruptedException{
        if(zooKeeper!=null){
            zooKeeper.close();
        }
    }
}
