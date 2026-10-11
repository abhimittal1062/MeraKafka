package com.merakafka.broker;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

public class Protocol {
    private Protocol() {

    }

    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker Response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x1F;

    // Internal Broker Communication
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIFICATION = 0x23;

    private static final int MAX_MESSAGE=32*1024*1024;
    private static final int MAX_TOPIC_SIZE=Short.MAX_VALUE;

    private static byte[] encodeTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be null or blank");
        }

        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        if (topicBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Topic name is too long");
        }

        return topicBytes;
    }

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = encodeTopic(topic);

        if (partition < 0) {
            throw new IllegalArgumentException("Partition cant be negative");
        }

        if (message == null) {
            throw new IllegalArgumentException("Message cant be Null");
        }

        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 4 + message.length);

        buffer.put(PRODUCE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = encodeTopic(topic);

        if (partition < 0) {
            throw new IllegalArgumentException("Partition cant be negative");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("Offset cant be negative");
        }

        if (maxBytes <= 0) {
            throw new IllegalArgumentException("MaxBytes cant be negative");
        }

        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 8 + 4);

        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = encodeTopic(topic);

        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Partitions cant be negative");
        }

        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("Replications cant be negative");
        }

        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4 + 2);

        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        byte[] topicBytes = encodeTopic(topic);

        if (partition < 0) {
            throw new IllegalArgumentException("Partition cant be negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset cant be negative");
        }
        if (message == null) {
            throw new IllegalArgumentException("message cant be null");
        }

        ByteBuffer buffer = ByteBuffer.allocate(
                1 + Short.BYTES + topicBytes.length + Integer.BYTES + Long.BYTES + Integer.BYTES + message.length);

        buffer.put(REPLICATE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = encodeTopic(topic);

        ByteBuffer buffer = ByteBuffer.allocate(1 + Short.BYTES + topicBytes.length);

        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);

        buffer.flip();
        return buffer;
    }

    public static final class ProduceResult {
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long getOffset() {
            return offset;
        }

        public String getError() {
            return error;
        }

        public boolean hasError() {
            return error != null;
        }
    }

    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Response buffer cannot be null");
        }
        try {
            if (buffer.remaining() < 1) {
                throw new IllegalArgumentException("Empty Produce Message");
            }
            byte responseType = buffer.get();
            if (responseType == ERROR_RESPONSE) {
                if (buffer.remaining() < Short.BYTES) {
                    throw new IllegalArgumentException("Incomplete Error Response");
                }

                short errorLength = buffer.getShort();

                if (errorLength < 0 || errorLength != buffer.remaining()) {
                    throw new IllegalArgumentException("Invalid error length");
                }

                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes, StandardCharsets.UTF_8);

                return new ProduceResult(-1, error);
            }
            if (responseType != PRODUCE_RESPONSE) {
                throw new IllegalArgumentException("Unexpected Response Type: " + responseType);
            }

            // 8 bytes for offset and 1 byte for status
            if (buffer.remaining() < Long.BYTES + 1) {
                throw new IllegalArgumentException("Incomplete Produce Response");
            }

            long offset = buffer.getLong();
            byte status = buffer.get();
            if (status == 0) {
                if (offset < 0) {
                    throw new IllegalArgumentException("Invalid Message Offset: " + offset);
                }
                if (buffer.hasRemaining()) {
                    throw new IllegalArgumentException("Unexpected Trailing bytes in Produce Response");
                }
                return new ProduceResult(offset, null);
            }

            if (buffer.hasRemaining()) {
                throw new IllegalArgumentException("Unexpected Trailing bytes in Produce Response");
            }

            return new ProduceResult(offset, "Produce Failed");
        } catch (java.nio.BufferUnderflowException e) {
            throw new IllegalArgumentException("Malformed Produce Response", e);
        }
    }

    public static final class FetchMessage {
        private final long offset;
        private final byte[] value;

        public FetchMessage(long offset, byte[] value) {
            if (offset < 0) {
                throw new IllegalArgumentException("Offset cant be negative");
            }
            if (value == null) {
                throw new IllegalArgumentException("Value cant be null");
            }

            this.offset = offset;
            this.value = value.clone();
        }

        public long getOffset() {
            return offset;
        }

        public byte[] getValue() {
            return value.clone();
        }
    }

    public static final class FetchResult {
        private final List<FetchMessage> messages;
        private final String error;

        public FetchResult(List<FetchMessage> messages, String error) {
            this.messages = messages == null ? new ArrayList<>() : new ArrayList<>(messages);
            this.error = error;
        }

        public List<FetchMessage> getMessages() {
            return Collections.unmodifiableList(messages);
        }

        public String getError() {
            return error;
        }

        public boolean hasError() {
            return error != null;
        }
    }

    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Response buffer cant be null");
        }

        try {
            if (!buffer.hasRemaining()) {
                throw new IllegalArgumentException("Empty Fetch Response");
            }

            byte responseType = buffer.get();

            if (responseType == ERROR_RESPONSE) {
                if (buffer.remaining() < Short.BYTES) {
                    throw new IllegalArgumentException("Incomplete Error Response");
                }

                short errorLength = buffer.getShort();

                if (errorLength < 0 || errorLength != buffer.remaining()) {
                    throw new IllegalArgumentException("Invalid error Message length");
                }

                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes, StandardCharsets.UTF_8);

                return new FetchResult(null, error);
            }

            if (responseType != FETCH_RESPONSE) {
                throw new IllegalArgumentException("Unexpected response type: " + responseType);
            }

            if (buffer.remaining() < Integer.BYTES) {
                throw new IllegalArgumentException("Missing message Count");
            }

            int messageCount = buffer.getInt();

            if (messageCount < 0) {
                throw new IllegalArgumentException("Message Count cannot be negative");
            }

            // if every message requires atleast 8 bytes for offset and 4 bytes for message
            // length
            if (messageCount > buffer.remaining() / 12) {
                throw new IllegalArgumentException("Invalid message count for response size");
            }

            List<FetchMessage> messages = new ArrayList<>();

            for (int i = 0; i < messageCount; i++) {
                if (buffer.remaining() < Long.BYTES + Integer.BYTES) {
                    throw new IllegalArgumentException("Incomplete message metadata at index " + i);
                }
                long offset = buffer.getLong();
                int messageLength = buffer.getInt();

                if (offset < 0) {
                    throw new IllegalArgumentException("Negative offset at index " + i);
                }

                if (messageLength < 0 || messageLength > buffer.remaining()) {
                    throw new IllegalArgumentException("Invalid message length at index " + i);
                }

                byte[] messageBytes = new byte[messageLength];
                buffer.get(messageBytes);

                messages.add(new FetchMessage(offset, messageBytes));
            }

            if (buffer.hasRemaining()) {
                throw new IllegalArgumentException("Unexpected trailing bytes in fetch memory");
            }

            return new FetchResult(messages, null);
        } catch (java.nio.BufferUnderflowException e) {
            throw new IllegalArgumentException("Malformed Fetch Response: incomplete data", e);
        }
    }

    public static final class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = new ArrayList<>(brokers);
            this.topics = new ArrayList<>(topics);
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() {
            return Collections.unmodifiableList(brokers);
        }

        public List<TopicMetadata> getTopics() {
            return Collections.unmodifiableList(topics);
        }

        public String getError() {
            return error;
        }

        public boolean hasError() {
            return error != null;
        }
    }

    public static final class TopicMetadata {
        private final String name;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = new ArrayList<>(partitions);
        }

        public String getName() {
            return name;
        }

        public List<PartitionMetadata> getPartitions() {
            return Collections.unmodifiableList(partitions);
        }
    }

    public static final class PartitionMetadata {
        private final int id;
        private final int leader;
        private final List<Integer> replicas;

        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = new ArrayList<>(replicas);
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getReplicas() {
            return Collections.unmodifiableList(replicas);
        }
    }

    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Response buffer cannot be null");
        }

        try {
            if (!buffer.hasRemaining()) {
                throw new IllegalArgumentException("Empty Metadata response");
            }

            byte responseType = buffer.get();

            // Error Response: type+2-byte length+UTF-8 bytes
            if (responseType == ERROR_RESPONSE) {
                if (buffer.remaining() < Short.BYTES) {
                    throw new IllegalArgumentException("Incomplete Error Response");
                }

                short errorLength = buffer.getShort();

                if (errorLength < 0 || errorLength != buffer.remaining()) {
                    throw new IllegalArgumentException("Invalid error Message length");
                }

                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes, StandardCharsets.UTF_8);
                return new MetadataResult(new ArrayList<>(), new ArrayList<>(), error);
            }

            if (responseType != METADATA_RESPONSE) {
                throw new IllegalArgumentException("Unexpected response type: " + responseType);
            }

            if (buffer.remaining() < Integer.BYTES) {
                throw new IllegalArgumentException("Missing Broker count");
            }

            int brokerCount = buffer.getInt();

            if (brokerCount < 0 || brokerCount > buffer.remaining() / 10) {
                throw new IllegalArgumentException("Invalid Broker Count");
            }

            List<BrokerInfo> brokers = new ArrayList<>();

            for (int i = 0; i < brokerCount; i++) {
                if (buffer.remaining() < Integer.BYTES + Short.BYTES) {
                    throw new IllegalArgumentException("Incomplete BrokerInfo for broker " + i);
                }

                int brokerId = buffer.getInt();
                short hostLength = buffer.getShort();
                if (hostLength <= 0 || hostLength > buffer.remaining() - Integer.BYTES) {
                    throw new IllegalArgumentException("Invalid broker host length at index " + i);
                }

                byte[] hostBytes = new byte[hostLength];
                buffer.get(hostBytes);
                String host = new String(hostBytes, StandardCharsets.UTF_8);
                int port = buffer.getInt();
                brokers.add(new BrokerInfo(brokerId, host, port));
            }

            if (buffer.remaining() < Integer.BYTES) {
                throw new IllegalArgumentException("Missing Topic Count");
            }

            int topicCount = buffer.getInt();

            if (topicCount < 0 || topicCount > buffer.remaining() / 6) {
                throw new IllegalArgumentException("Invalid Topic count");
            }

            List<TopicMetadata> topics = new ArrayList<>();

            for (int i = 0; i < topicCount; i++) {
                if (buffer.remaining() < Short.BYTES) {
                    throw new IllegalArgumentException("Missing topic names at index " + i);
                }
                short topicNameLength = buffer.getShort();
                if (topicNameLength <= 0 || topicNameLength > buffer.remaining() - Integer.BYTES) {
                    throw new IllegalArgumentException("Invalid topic name length at index " + i);
                }

                byte[] topicBytes = new byte[topicNameLength];
                buffer.get(topicBytes);
                String topicName = new String(topicBytes, StandardCharsets.UTF_8);
                int partitionCount = buffer.getInt();

                if (partitionCount < 0 || partitionCount > buffer.remaining() / 12) {
                    throw new IllegalArgumentException("Invalid partition count for topic " + topicName);
                }

                List<PartitionMetadata> partitions = new ArrayList<>();

                for (int j = 0; j < partitionCount; j++) {
                    if (buffer.remaining() < 3 * Integer.BYTES) {
                        throw new IllegalArgumentException("Incomplete partition metadata for topic " + topicName);
                    }
                    int partitionId = buffer.getInt();
                    int leader = buffer.getInt();
                    int replicaCount = buffer.getInt();

                    if (partitionId < 0 || leader < 0) {
                        throw new IllegalArgumentException("Invalid partition Id or leader ID");
                    }
                    if (replicaCount < 0 || replicaCount > buffer.remaining() / Integer.BYTES) {
                        throw new IllegalArgumentException("Invalid replica count for partition " + partitionId);
                    }
                    List<Integer> replicaIds = new ArrayList<>();
                    for (int k = 0; k < replicaCount; k++) {
                        int replicaId = buffer.getInt();
                        if (replicaId < 0) {
                            throw new IllegalArgumentException("Invalid replica Id");
                        }
                        replicaIds.add(replicaId);
                    }

                    partitions.add(new PartitionMetadata(partitionId, leader, replicaIds));
                }
                topics.add(new TopicMetadata(topicName, partitions));
            }
            if (buffer.hasRemaining()) {
                throw new IllegalArgumentException("Unexpected trailing bytes in metadata response");
            }

            return new MetadataResult(brokers, topics, null);
        }

        catch (java.nio.BufferUnderflowException e) {
            throw new IllegalArgumentException("Malformed Metadata response: incomplete data", e);
        }
    }

    public static final class Request{
        private final byte type;
        private final String topic;
        private final int partition;
        private final long offset;
        private final int maxBytes;
        private final byte[] message;
        private final int numPartitions;
        private final short replicationFactor;

        private Request(byte type, String topic, int partition, long offset, int maxBytes, byte[] message, int numPartitions, short replicationFactor) {
            this.type=type;
            this.topic=topic;
            this.partition=partition;
            this.offset=offset;
            this.maxBytes=maxBytes;
            this.message=message==null?null:message.clone();
            this.numPartitions=numPartitions;
            this.replicationFactor=replicationFactor;
        }

        public byte getType() {
            return type;
        }
        public String getTopic() {
            return topic;
        }
        public int getPartition() {
            return partition;
        }
        public long getOffset() {
            return offset;
        }
        public int getMaxBytes() {
            return maxBytes;
        }
        public byte[] getMessage() {
            return message==null?null:message.clone();
        }
        public int getNumPartitions() {
            return numPartitions;
        }
        public short getReplicationFactor() {
            return replicationFactor;
        }
    }

    private static String readTopic(DataInputStream in) throws IOException {
        short length=in.readShort();

        if(length<=0 || length>MAX_TOPIC_SIZE) {
            throw new IOException("Invalid topic length: "+length);
        }

        byte[] bytes=new byte[length];
        in.readFully(bytes);
        String topic=new String(bytes, StandardCharsets.UTF_8);
        if (topic.isBlank() || topic.length()>MAX_TOPIC_SIZE) {
            throw new IOException("Invalid Topic name");
        }
        return topic;
    }

    public static Request readRequest(InputStream input) throws IOException{
        DataInputStream in=new DataInputStream(input);

        int requestType=in.read();
        if(requestType==-1) {
            return null;
        }

        byte type=(byte) requestType;

        switch (type) {
            case PRODUCE: {
                String topic=readTopic(in);
                int partition=in.readInt();
                int messageLength=in.readInt();

                if(partition<0) {
                    throw new IOException("Invalid partition");
                }
                if(messageLength<0 || messageLength>MAX_MESSAGE) {
                    throw new IOException("Invalid message Length: "+messageLength);
                }

                byte[] message=new byte[messageLength];

                in.readFully(message);

                return new Request(type, topic, partition, 0, 0, message, 0, (short) 0);
            }

            case FETCH: {
                String topic=readTopic(in);
                int partition=in.readInt();
                long offset=in.readLong();
                int maxBytes=in.readInt();

                if(partition<0 || offset<0 || maxBytes<=0 || maxBytes>MAX_MESSAGE) {
                    throw new IOException("Invalid FETCH parameters");
                }

                return new Request(type, topic, partition, offset, maxBytes, null, 0, (short) 0);
            }

            case REPLICATE: {
                String topic=readTopic(in);
                int partition=in.readInt();
                long offset=in.readLong();
                int messageLength=in.readInt();

                if(partition<0 || offset<0) {
                    throw new IOException("Invalid REPLICATE partition or offset");
                }

                if(messageLength<0 || messageLength>MAX_MESSAGE) {
                    throw new IOException("Invalid Replicate message length: "+messageLength);
                }

                byte[] message=new byte[messageLength];

                in.readFully(message);

                return new Request(type,topic,partition,offset,0,message,0,(short)0);
            }
                
            case METADATA: {
                return new Request(type, null, 0, 0, 0, null, 0, (short) 0);
            }

            case CREATE_TOPIC: {
                String topic=readTopic(in);
                int partitions=in.readInt();
                short replicationFactor=in.readShort();

                if(partitions<0 || replicationFactor<=0) {
                    throw new IOException("Invalid CREATE_TOPIC parameters");
                }

                return new Request(type,topic,0,0,0,null,partitions,replicationFactor);
            }
        
            default:
                throw new IOException(String.format("Unknown request type: 0x%02X", type));
        }
    }

    public static ByteBuffer encodeProduceResponse(long offset,boolean success) {
        ByteBuffer buffer=ByteBuffer.allocate(1+Long.BYTES+1);
        buffer.put(PRODUCE_RESPONSE);
        buffer.putLong(offset);
        buffer.put((byte) (success?0:1));
        
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeFetchResponse(List<FetchMessage> messages) {
        if(messages==null) {
            throw new IllegalArgumentException("Messages cant be null");
        }

        ByteArrayOutputStream bytes=new ByteArrayOutputStream();

        try(java.io.DataOutputStream out=new java.io.DataOutputStream(bytes)) {
            out.writeByte(FETCH_RESPONSE);
            out.writeInt(messages.size());

            for(FetchMessage message:messages) {
                byte[] value=message.getValue();
                out.writeLong(message.getOffset());
                out.writeInt(value.length);
                out.write(value);
            }
        }
        catch(IOException e) {
            throw new IllegalStateException("Could not encode Fetch response",e);
        }

        return ByteBuffer.wrap(bytes.toByteArray());
    }

    public static ByteBuffer encodeCreateTopicResponse() {
        ByteBuffer buffer=ByteBuffer.allocate(1);
        buffer.put(CREATE_TOPIC_RESPONSE);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeErrorResponse(String errorMessage) {
        if(errorMessage==null) {
            errorMessage="Unknown error";
        }

        byte[] errorBytes=errorMessage.getBytes(StandardCharsets.UTF_8);

        if(errorBytes.length>Short.MAX_VALUE) {
            throw new IllegalArgumentException("Error message too long");
        }

        ByteBuffer buffer=ByteBuffer.allocate(1+Short.BYTES+errorBytes.length);
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) errorBytes.length);
        buffer.put(errorBytes);

        buffer.flip();
        return buffer;
    }

    public static void writeResponse(java.io.OutputStream output,ByteBuffer response) throws IOException{
        ByteBuffer buffer=response.asReadOnlyBuffer();
        byte[] bytes=new byte[buffer.remaining()];

        buffer.get(bytes);
        output.write(bytes);
        output.flush();
    }

    

    public static ByteBuffer encodeMetadataResponse(List<BrokerInfo> brokers,List<TopicMetadata> topics) {
        if(brokers==null || topics==null) {
            throw new IllegalArgumentException("Brokers and topics cant be null");
        }

        ByteArrayOutputStream bytes=new ByteArrayOutputStream();

        try(java.io.DataOutputStream out=new java.io.DataOutputStream(bytes)) {
            out.writeByte(METADATA_RESPONSE);

            out.writeInt(brokers.size());

            for(BrokerInfo broker:brokers) {
                byte[] hostBytes=broker.getHost().getBytes(StandardCharsets.UTF_8);

                if(hostBytes.length==0 || hostBytes.length>Short.MAX_VALUE) {
                    throw new IllegalArgumentException("Invalid broker host length");
                }

                out.writeInt(broker.getBrokerId());
                out.writeShort(hostBytes.length);
                out.write(hostBytes);
                out.writeInt(broker.getPort());
            }

            out.writeInt(topics.size());
            for(TopicMetadata topic:topics) {
                byte[] topicBytes=topic.getName().getBytes(StandardCharsets.UTF_8);

                out.writeShort(topicBytes.length);
                out.write(topicBytes);

                List<PartitionMetadata> partitions=topic.getPartitions();
                out.writeInt(partitions.size());

                for(PartitionMetadata partition:partitions) {
                    out.writeInt(partition.getId());
                    out.writeInt(partition.getLeader());

                    List<Integer> replicas=partition.getReplicas();
                    out.writeInt(replicas.size());

                    for(int replicaId:replicas) {
                        out.writeInt(replicaId);
                    }
                }
            }
            out.flush();
        }
        catch(IOException e) {
            throw new IllegalStateException("Could not encode metadata response",e);
        }

        return ByteBuffer.wrap(bytes.toByteArray());
    }
}
