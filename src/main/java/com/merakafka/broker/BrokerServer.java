package com.merakafka.broker;

import com.merakafka.zookeeper.ZooKeeperClient;

import com.merakafka.storage.TopicManager;
import com.merakafka.Message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.DataInputStream;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.net.ProtocolException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;


public class BrokerServer implements AutoCloseable{
    private final BrokerInfo brokerInfo;
    private final ZooKeeperClient zooKeeperClient;
    private final ExecutorService clientPool;
    private final ServerSocket serverSocket;
    private final TopicManager topicManager;
    private final ScheduledExecutorService leaderMonitor=Executors.newSingleThreadScheduledExecutor();

    public BrokerServer(BrokerInfo brokerInfo, String zooKeeperAddress) throws IOException, InterruptedException, org.apache.zookeeper.KeeperException{
        this.brokerInfo=brokerInfo;
        this.serverSocket=new ServerSocket(brokerInfo.getPort());
        this.clientPool= Executors.newCachedThreadPool();
        this.topicManager=new TopicManager(Path.of("data","broker-"+brokerInfo.getBrokerId()));

        try {
            this.zooKeeperClient= new ZooKeeperClient(zooKeeperAddress, 5000);
            zooKeeperClient.registerBroker(brokerInfo);
            initializeLocalTopicMetadata();
            leaderMonitor.scheduleWithFixedDelay(this::checkLeaderFailure, 1, 1, TimeUnit.SECONDS);
        }
        catch(IOException | InterruptedException | org.apache.zookeeper.KeeperException e) {
            serverSocket.close();
            clientPool.shutdownNow();
            throw e;
        }

        System.out.println("Mera-Kafka Broker "+brokerInfo.getBrokerId()+" started on "+brokerInfo.getHost()+":"+brokerInfo.getPort());
    }

    public void start() throws IOException{
        System.out.println("Waiting for Client Connections");
        
        while(!serverSocket.isClosed()) {
            Socket clientSocket=serverSocket.accept();
            System.out.println("Client Connected: "+clientSocket.getRemoteSocketAddress());

            clientPool.submit(()->handleClient(clientSocket));
        }
    }

    public void handleClient(Socket socket) {
        try(socket) {
            Protocol.Request request=Protocol.readRequest(socket.getInputStream());

            if(request==null) {
                return;
            }

            System.out.printf("Received request type: 0x%02X from %s%n", request.getType(), socket.getRemoteSocketAddress());
            if(request.getType()==Protocol.METADATA) {
                System.out.println("Debug: Metadata Request Received");
            }

            ByteBuffer response;

            switch(request.getType()) {
                case Protocol.PRODUCE: {
                    if(request.getPartition()!=0) {
                        response=Protocol.encodeErrorResponse("Only partition 0 is supported currently");
                        break;
                    }

                    String topic=request.getTopic();
                    String metadata=zooKeeperClient.getTopicMetadata(topic);

                    if(metadata==null) {
                        response=Protocol.encodeErrorResponse("Unknown topic: "+topic);
                        break;
                    }

                    String[] parts=metadata.split("\\|",2);

                    if(parts.length!=2) {
                        response=Protocol.encodeErrorResponse("Invalid topic metadata");
                        break;
                    }
                    int leaderId=Integer.parseInt(parts[0]);
                    if(leaderId!=brokerInfo.getBrokerId()) {
                        response=Protocol.encodeErrorResponse("Not the leader for topic: '"+topic+"'. Leader broker ID: "+leaderId);
                        break;
                    }

                    Message message=new Message(topic, new String(request.getMessage(),StandardCharsets.UTF_8));
                    long offset=topicManager.append(topic, message);

                    try {
                        List<String> activeBrokers=zooKeeperClient.getRegisteredBrokers();
                        for(String replica:parts[1].split(",")) {
                            int replicaId=Integer.parseInt(replica.trim());
                            if(replicaId==brokerInfo.getBrokerId()) {
                                continue;
                            }

                            //Skip replicas that are currently offline
                            if(!activeBrokers.contains(String.valueOf(replicaId))) {
                                System.err.println("Skipping unavailable replica brokers "+replicaId);
                                continue;
                            }


                            BrokerInfo replicaBroker=findBroker(replicaId);
                            ByteBuffer replicationRequest=Protocol.encodeReplicateRequest(topic, request.getPartition(), offset, request.getMessage());

                            replicateToBroker(replicaBroker,replicationRequest);
                        }
                        response=Protocol.encodeProduceResponse(offset,true);
                        System.out.printf("Stored and replicated message in Topic %s at offset %d%n",request.getTopic(),offset);
                    }
                    catch(IOException e) {
                        response=Protocol.encodeErrorResponse("Replication failed at offset "+offset+": "+e.getMessage());
                    }
                    break;
                }

                case Protocol.FETCH: {
                    if(request.getPartition()!=0) {
                        response=Protocol.encodeErrorResponse("Only partition 0 is supported currently");
                        break;
                    }

                    if(request.getOffset()<0) {
                        response=Protocol.encodeErrorResponse("Offset cant be negative");
                        break;
                    }

                    List<Protocol.FetchMessage> messages=new ArrayList<>();
                    long offset=request.getOffset();
                    int bytesRead=0;
                    while (offset<topicManager.size(request.getTopic())) {
                        Message message=topicManager.read(request.getTopic(),offset);

                        if(message==null) {
                            break;
                        }

                        byte[] value=message.getValue().getBytes(StandardCharsets.UTF_8);
                        
                        //each entry needs 8 bytes for offset anf 4 bytes for length
                        if(bytesRead+Long.BYTES+Integer.BYTES+value.length>request.getMaxBytes()) {
                            break;
                        }

                        messages.add(new Protocol.FetchMessage(offset, value));
                        bytesRead+=Long.BYTES+Integer.BYTES+value.length;
                        offset++;
                    }

                    response=Protocol.encodeFetchResponse(messages);
                    System.out.printf("Fetched %d messages from topic '%s'%n",messages.size(),request.getTopic());
                    break;
                }

                case Protocol.REPLICATE: {
                    if(request.getPartition()!=0) {
                        response=Protocol.encodeErrorResponse("Only Partition 0 is supported");
                        break;
                    }

                    String topic=request.getTopic();
                    if(!topicManager.getTopicNames().contains(topic)) {
                        topicManager.createTopic(topic);
                    }

                    long offset=request.getOffset();
                    int currentSize=topicManager.size(topic);

                    if(offset<currentSize) {
                        Message existing=topicManager.read(topic,offset);

                        if(existing!=null && existing.getValue().equals(new String(request.getMessage(),StandardCharsets.UTF_8))) {
                            response=ByteBuffer.wrap(new byte[]{Protocol.REPLICATE_ACK});
                        }
                        else {
                            response=Protocol.encodeErrorResponse("Conflicting messages at offset "+offset);
                        }

                        break;
                    }

                    if(offset>currentSize) {
                        response=Protocol.encodeErrorResponse("Replication offset gap: expected "+currentSize+", received "+offset);
                        break;
                    }

                    Message replicatedMessage=new Message(topic, new String(request.getMessage(),StandardCharsets.UTF_8));
                    long storedOffset=topicManager.append(topic,replicatedMessage);

                    if(storedOffset!=offset) {
                        response=Protocol.encodeErrorResponse("Replicated offset mismatch");
                        break;
                    }
                    response=ByteBuffer.wrap(new byte[]{Protocol.REPLICATE_ACK});
                    System.out.printf("Replica broker %d stored topic '%s' at offset %d%n", brokerInfo.getBrokerId(),topic,offset);
                    break;
                }

                case Protocol.CREATE_TOPIC: {
                    if(request.getNumPartitions()!=1) {
                        response=Protocol.encodeErrorResponse("Currently only 1 partition is supported");
                        break;
                    }

                    String topicName=request.getTopic();
                    int replicationFactor=request.getReplicationFactor();

                    if(topicName==null || !topicName.matches("[a-zA-Z0-9._-]+")) {
                        response=Protocol.encodeErrorResponse("Invalid topic Name");
                        break;
                    }

                    List<String> registeredIds=zooKeeperClient.getRegisteredBrokers();

                    if(replicationFactor>registeredIds.size()) {
                        response=Protocol.encodeErrorResponse("Not enough registered brokers for replication factor");
                        break;
                    }

                    List<Integer> replicas=new ArrayList<>();
                    replicas.add(brokerInfo.getBrokerId());

                    for(String registeredId:registeredIds) {
                        int id=Integer.parseInt(registeredId);
                        if(id!=brokerInfo.getBrokerId() && replicas.size()<replicationFactor) {
                            replicas.add(id);
                        }
                    }

                    String replicaList=replicas.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(","));

                    String metadata=brokerInfo.getBrokerId()+"|"+replicaList;

                    boolean created=zooKeeperClient.createTopicMetadataIfAbsent(topicName, metadata);

                    if(!created) {
                        response=Protocol.encodeErrorResponse("Topic Already Exists: "+topicName);
                        break;
                    }

                    topicManager.createTopic(topicName);
                    response=Protocol.encodeCreateTopicResponse();
                    System.out.printf("Created Topic:%s, leader=%d, replicas=%s%n ",topicName,brokerInfo.getBrokerId(),replicaList);
                    break;
                }
                case Protocol.METADATA: {
                    List<String> brokerIds=zooKeeperClient.getRegisteredBrokers();
                    List<BrokerInfo> brokers=new ArrayList<>();
                    List<Protocol.TopicMetadata> topics=new ArrayList<>();

                    for(String brokerId:brokerIds) {
                        String brokerPath="/merakafka/brokers/"+brokerId;
                        String brokerData=zooKeeperClient.getData(brokerPath);
                        String[] address=brokerData.split(":",2);

                        if(address.length!=2) {
                            throw new IOException("Invaild broker data for broker "+brokerId);
                        }
                        String host=address[0];

                        int port=Integer.parseInt(address[1]);
                        brokers.add(new BrokerInfo(Integer.parseInt(brokerId),host,port));
                    }

                    for(String topicName:zooKeeperClient.getTopicNames()) {
                        String metadata=zooKeeperClient.getTopicMetadata(topicName);

                        if(metadata==null) {
                            continue;
                        }

                        String[] parts=metadata.split("\\|",2);
                        if(parts.length!=2) {
                            throw new IOException("Invalid metadata for topic: "+topicName);
                        }

                        int leaderId=Integer.parseInt(parts[0]);
                        List<Integer> replicas=new ArrayList<>();

                        for(String replica:parts[1].split(",")) {
                            replicas.add(Integer.parseInt(replica));
                        }

                        List<Protocol.PartitionMetadata> partitions=List.of(new Protocol.PartitionMetadata(0, leaderId, replicas));
                        topics.add(new Protocol.TopicMetadata(topicName, partitions));
                    }

                    response=Protocol.encodeMetadataResponse(brokers,topics);
                    System.out.printf("Returned Metadata for %d brokers and %d topics%n",brokers.size(),topics.size());
                    break;
                }
                default: {
                    response=Protocol.encodeErrorResponse("Unknown request type");
                }
            }

            Protocol.writeResponse(socket.getOutputStream(),response);
        }
        catch(IOException e) {
            System.err.println("Request Failed: "+e.getMessage());

            try(socket) {
                Protocol.writeResponse(socket.getOutputStream(), Protocol.encodeErrorResponse(e.getMessage()==null?"Request Failed" :e.getMessage()));
            }
            catch(IOException ex) {
                //The connection may already be closed
            }
        }
        catch(org.apache.zookeeper.KeeperException e) {
            System.err.println("ZooKeeper request failed: "+e.getMessage());

            try {
                Protocol.writeResponse(socket.getOutputStream(), Protocol.encodeErrorResponse("Zookeeper request failed"));
            }
            catch(IOException ex) {
                //connection may already be closed
            }
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Request interrupted: "+e.getMessage());

            try {
                Protocol.writeResponse(socket.getOutputStream(), Protocol.encodeErrorResponse("Request interrupted"));
            }
            catch(IOException ex) {
                //connection may already be closed
            }
        }
        catch(RuntimeException e) {
            System.err.println("Unexpected broker error");
            e.printStackTrace();

            try {
                Protocol.writeResponse(socket.getOutputStream(), Protocol.encodeErrorResponse("Internal server error"));
            }
            catch(IOException ex) {
                //connection may already be closed
                System.err.println("Could not send error response: " + ex.getMessage());
            }
        }
    }

    private BrokerInfo findBroker(int brokerId) throws org.apache.zookeeper.KeeperException,InterruptedException,IOException{
        String path="/merakafka/brokers/"+brokerId;

        String data=zooKeeperClient.getData(path);
        if(data==null) {
            throw new IOException("Broker not registered: "+brokerId);
        }

        String[] address=data.split(":",2);

        if(address.length!=2) {
            throw new IOException("Invalid broker address: "+data);
        }

        return new BrokerInfo(brokerId,address[0],Integer.parseInt(address[1]));
    }

    private void replicateToBroker(BrokerInfo replica,ByteBuffer request) throws IOException {
        try(Socket socket=new Socket()) {
            socket.connect(new InetSocketAddress(replica.getHost(),replica.getPort()),3000);
            socket.setSoTimeout(5000);

            java.io.OutputStream output=socket.getOutputStream();

            ByteBuffer buffer=request.asReadOnlyBuffer();
            byte[] bytes=new byte[buffer.remaining()];
            buffer.get(bytes);
            output.write(bytes);
            output.flush();

            DataInputStream input=new DataInputStream(socket.getInputStream());
            int responseType=input.read();

            if(responseType==Protocol.REPLICATE_ACK) {
                return;
            }
            if(responseType==Protocol.ERROR_RESPONSE) {
                short length=input.readShort();
                if(length<0) {
                    throw new IOException("Invalid replica error response");
                }
                byte[] errroBytes=new byte[length];
                input.readFully(errroBytes);
                throw new IOException(new String(errroBytes, StandardCharsets.UTF_8));
            }

            throw new IOException("Unexpected response from replica "+replica.getBrokerId()+": "+responseType);
        }
    }

    private void catchUpFromLeader(String topic,int leaderId) throws Exception {
        BrokerInfo leader=findBroker(leaderId);
        if(!topicManager.getTopicNames().contains(topic)) {
            topicManager.createTopic(topic);
        }

        while(true) {
            long nextOffset=topicManager.size(topic);

            ByteBuffer request=Protocol.encodeFetchRequest(topic, 0, nextOffset, 1024*1024);

            try(Socket socket=new Socket()) {
                socket.connect(new InetSocketAddress(leader.getHost(),leader.getPort()),3000);

                socket.setSoTimeout(5000);

                ByteBuffer buffer=request.asReadOnlyBuffer();
                byte[] requestBytes=new byte[buffer.remaining()];
                buffer.get(requestBytes);

                socket.getOutputStream().write(requestBytes);
                socket.getOutputStream().flush();

                DataInputStream input=new DataInputStream(socket.getInputStream());

                int responseType=input.read();

                if(responseType==-1) {
                    throw new IOException("Leader closed connection");
                }

                if(responseType==Protocol.ERROR_RESPONSE) {
                    short length=input.readShort();

                    if(length<0) {
                        throw new IOException("Invalid leader error response");
                    }

                    byte[] errorBytes=new byte[length];
                    input.readFully(errorBytes);

                    throw new IOException(new String(errorBytes,StandardCharsets.UTF_8));
                }

                if(responseType!=Protocol.FETCH_RESPONSE) {
                    throw new IOException("Unexpected fetch response: "+responseType);
                }

                int count=input.readInt();

                if(count<0) {
                    throw new IOException("Invalid fetch message count");
                }

                if(count==0) {
                    return;
                }

                for(int i=0;i<count;i++) {
                    long offset=input.readLong();
                    int length=input.readInt();

                    if(length<0 || length>32*1024*1024) {
                        throw new IOException("Invalid message length");
                    }

                    byte[] value=new byte[length];
                    input.readFully(value);

                    long expectedOffset=topicManager.size(topic);

                    if(offset!=expectedOffset) {
                        throw new IOException("Catch-up offset mismatch: expected "+expectedOffset+", received "+offset);
                    }

                    Message message=new Message(topic, new String(value,StandardCharsets.UTF_8));

                    long storedOffset=topicManager.append(topic,message);

                    if(storedOffset!=offset) {
                        throw new IOException("Stored offset mismatch during catch-up");
                    }

                    System.out.printf("Replica catch-up: broker=%d, topic=%s, offset=%d%n",brokerInfo.getBrokerId(),topic,offset);
                }
            }
        }
    }

    private void checkLeaderFailure() {
        try {
            List<String> liveBrokers=zooKeeperClient.getRegisteredBrokers();

            for(String topic:zooKeeperClient.getTopicNames()) {
                String metadata=zooKeeperClient.getTopicMetadata(topic);

                if(metadata==null)
                    continue;

                String[] parts=metadata.split("\\|",2);
                if(parts.length!=2) {
                    continue;
                }

                int currentLeader=Integer.parseInt(parts[0]);

                //Current leader is alive: catch-up if this broker is replica
                if(liveBrokers.contains(String.valueOf(currentLeader))) {
                    boolean isReplica=false;

                    for(String replica:parts[1].split(",")) {
                        if(Integer.parseInt(replica.trim())==brokerInfo.getBrokerId()) {
                            isReplica=true;
                            break;
                        }
                    }

                    if(isReplica && currentLeader!=brokerInfo.getBrokerId()) {
                        try {
                            catchUpFromLeader(topic, currentLeader);
                        }
                        catch(Exception e) {
                            System.err.printf("Replica catch-up failed: topic=%s, error=%s%n", topic,e.getMessage());
                        }
                    }

                    continue;
                }

                //Select the first available broker from the replica list
                String[] replicas=parts[1].split(",");

                Integer newLeader=null;

                for(String replica:replicas) {
                    String replicaId=replica.trim();
                    if(liveBrokers.contains(replicaId)) {
                        newLeader=Integer.parseInt(replicaId);
                        break;
                    }
                }

                if(newLeader==null) {
                    continue;
                }

                // Only the selected broker updates the leader metadata
                if(newLeader==brokerInfo.getBrokerId()) {
                    String updatedMetadata=newLeader+"|"+parts[1];

                    zooKeeperClient.updateTopicMetadata(topic, updatedMetadata);
                    System.out.printf("Leader failover: topic=%s, oldLeader=%d, newLeader=%d%n",topic,currentLeader,newLeader);
                }
            }
        }
        catch(Exception e) {
            if(!Thread.currentThread().isInterrupted()) {
                System.err.println("Leader monitor error: "+e.getMessage());
            }
        }
    }

    private void initializeLocalTopicMetadata() throws org.apache.zookeeper.KeeperException,InterruptedException {
        for(String topicName:topicManager.getTopicNames()) {
            String metadata=brokerInfo.getBrokerId()+"|"+brokerInfo.getBrokerId();
            zooKeeperClient.createTopicMetadataIfAbsent(topicName, metadata);
        }
    }

    @Override 
    public void close() throws IOException, InterruptedException {
        if(!serverSocket.isClosed()) {
            serverSocket.close();
        }

        clientPool.shutdown();
        zooKeeperClient.close();
        leaderMonitor.shutdownNow();
    }
    
    public static void main(String[] args) throws Exception {

        int brokerId=args.length>=1?Integer.parseInt(args[0]):1;
        String host=args.length>=2?args[1]:"localhost";
        int port=args.length>=3?Integer.parseInt(args[2]):9092;
        String zooKeeperAddress=args.length>=4?args[3]:"localhost:2181";
        BrokerInfo broker=new BrokerInfo(brokerId,host,port);
        BrokerServer server=new BrokerServer(broker, zooKeeperAddress);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            try {
                server.close();
            }
            catch(Exception e) {
                System.err.println("Error shutting down broker: "+e.getMessage());
            }
        }));

        server.start();
    }
}
