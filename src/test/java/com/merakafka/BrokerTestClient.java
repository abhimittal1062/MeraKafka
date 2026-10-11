
package com.merakafka;

import com.merakafka.broker.BrokerInfo;
import com.merakafka.broker.Protocol;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BrokerTestClient {
    private static int brokerPort = 9092;

    private static ByteBuffer sendRequest(ByteBuffer request)
            throws IOException {

        try (Socket socket = new Socket("localhost", brokerPort)) {
            socket.getOutputStream().write(
                request.array(),
                request.position(),
                request.remaining()
            );
            socket.getOutputStream().flush();

            DataInputStream in =
                new DataInputStream(socket.getInputStream());

            int responseType = in.readUnsignedByte();

            if (responseType == Protocol.ERROR_RESPONSE) {
                int length = in.readShort();

                if (length < 0) {
                    throw new IOException("Invalid error response length");
                }

                byte[] error = new byte[length];
                in.readFully(error);

                throw new IOException(
                    new String(error, StandardCharsets.UTF_8)
                );
            }

            if (responseType == Protocol.PRODUCE_RESPONSE) {
                long offset = in.readLong();
                byte status = in.readByte();

                ByteBuffer response =
                    ByteBuffer.allocate(Long.BYTES + 1);

                response.putLong(offset).put(status).flip();
                return response;
            }

            
            if (responseType == Protocol.FETCH_RESPONSE) {
                int count = in.readInt();

                if (count < 0) {
                    throw new IOException("Invalid fetch message count: " + count);
                }

                // Preserve the complete fetch response payload:
                // count + (offset + value length + value bytes) for each message.
                java.io.ByteArrayOutputStream bytes =
                        new java.io.ByteArrayOutputStream();

                try (java.io.DataOutputStream out =
                            new java.io.DataOutputStream(bytes)) {
                    out.writeInt(count);

                    for (int i = 0; i < count; i++) {
                        long offset = in.readLong();
                        int length = in.readInt();

                        if (length < 0) {
                            throw new IOException("Invalid message length: " + length);
                        }

                        byte[] value = new byte[length];
                        in.readFully(value);

                        out.writeLong(offset);
                        out.writeInt(length);
                        out.write(value);
                    }
                }

                return ByteBuffer.wrap(bytes.toByteArray());
            }

            if(responseType==Protocol.CREATE_TOPIC_RESPONSE) {
                return  ByteBuffer.wrap(new byte[]{(byte) responseType});
            }

            if (responseType == Protocol.METADATA_RESPONSE) {
                int brokerCount = in.readInt();

                if (brokerCount < 0) {
                    throw new IOException("Invalid broker count");
                }

                // Preserve the response type and payload for the decoder.
                ByteBuffer response = ByteBuffer.allocate(1024 * 1024);
                response.put((byte) responseType);
                response.putInt(brokerCount);

                for (int i = 0; i < brokerCount; i++) {
                    int id = in.readInt();
                    short hostLength = in.readShort();

                    if (hostLength <= 0) {
                        throw new IOException("Invalid broker host length");
                    }

                    byte[] hostBytes = new byte[hostLength];
                    in.readFully(hostBytes);
                    int port = in.readInt();

                    response.putInt(id);
                    response.putShort(hostLength);
                    response.put(hostBytes);
                    response.putInt(port);
                }

                int topicCount = in.readInt();
                response.putInt(topicCount);

                for (int i = 0; i < topicCount; i++) {
                    short nameLength = in.readShort();
                    byte[] nameBytes = new byte[nameLength];
                    in.readFully(nameBytes);

                    int partitionCount = in.readInt();

                    response.putShort(nameLength);
                    response.put(nameBytes);
                    response.putInt(partitionCount);

                    for (int j = 0; j < partitionCount; j++) {
                        int partitionId = in.readInt();
                        int leaderId = in.readInt();
                        int replicaCount = in.readInt();

                        response.putInt(partitionId);
                        response.putInt(leaderId);
                        response.putInt(replicaCount);

                        for (int k = 0; k < replicaCount; k++) {
                            response.putInt(in.readInt());
                        }
                    }
                }

                response.flip();
                return response;
            }

            throw new IOException(
                "Unexpected response type: " + responseType
            );
        }
    }

    
    private static void testMetadata() throws IOException {
        System.out.println("DEBUG: Sending metadata request to Broker on port " + brokerPort);

        ByteBuffer response = sendRequest(Protocol.encodeMetadataRequest());

        Protocol.MetadataResult metadata =
                Protocol.decodeMetadataResponse(response);

        System.out.println("\n--- METADATA TEST (BROKER " + brokerPort + ") ---");

        System.out.println("Registered brokers:");
        for (BrokerInfo broker : metadata.getBrokers()) {
            System.out.printf(
                    "Broker ID=%d, host=%s, port=%d%n",
                    broker.getBrokerId(),
                    broker.getHost(),
                    broker.getPort()
            );
        }

        System.out.println("Topics:");
        for (Protocol.TopicMetadata topic : metadata.getTopics()) {
            System.out.println("Topic: " + topic.getName());

            for (Protocol.PartitionMetadata partition : topic.getPartitions()) {
                System.out.printf(
                        "  Partition=%d, leader=%d, replicas=%s%n",
                        partition.getId(),
                        partition.getLeader(),
                        partition.getReplicas()
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {
        brokerPort = args.length > 0
                ? Integer.parseInt(args[0])
                : 9092;

        String topic = args.length > 1
                ? args[1]
                : "demo-new";

        System.out.println("Testing broker on port: " + brokerPort);
        System.out.println("Topic: " + topic);

        
        
    try {
        sendRequest(
            Protocol.encodeCreateTopicRequest(topic, 1, (short) 2)
        );
        System.out.println("TOPIC CREATED: " + topic);
    } catch (IOException e) {
        String error = e.getMessage();

        if (error != null && (
                error.contains("Topic Already Exists:")
                || error.contains("Topic created:")
                || error.contains("Not enough registered brokers")
        )) {
            System.out.println("Continuing with existing topic: " + topic);
        } else {
            throw e;
        }
    }



        String value = "Hello Mera-Kafka!";

        ByteBuffer produceResponse = sendRequest(
            Protocol.encodeProduceRequest(
                topic,
                0,
                value.getBytes(StandardCharsets.UTF_8)
            )
        );

        long offset = produceResponse.getLong();
        byte status = produceResponse.get();

        if (status != 0) {
            throw new IOException("Produce failed");
        }

        System.out.println(
            "\nPRODUCE SUCCESS: offset=" + offset + ", value=" + value
        );

        ByteBuffer fetchResponse = sendRequest(
            Protocol.encodeFetchRequest(topic, 0, offset, 1024 * 1024)
        );

                
        int count = fetchResponse.getInt();

        if (count <= 0) {
            throw new IOException(
                "Fetch failed: expected at least one message, got " + count
            );
        }

        boolean foundExpectedMessage = false;

        for (int i = 0; i < count; i++) {
            long fetchedOffset = fetchResponse.getLong();
            int length = fetchResponse.getInt();

            if (length < 0 || length > fetchResponse.remaining()) {
                throw new IOException("Invalid fetched message length: " + length);
            }

            byte[] fetchedBytes = new byte[length];
            fetchResponse.get(fetchedBytes);

            String fetchedValue =
                new String(fetchedBytes, StandardCharsets.UTF_8);

            System.out.printf(
                "FETCHED: offset=%d, value=%s%n",
                fetchedOffset,
                fetchedValue
            );

            if (fetchedOffset == offset && value.equals(fetchedValue)) {
                foundExpectedMessage = true;
            }
        }

        if (!foundExpectedMessage) {
            throw new IOException(
                "Fetch verification failed: expected offset="
                    + offset + ", value=" + value
            );
        }

        System.out.println("PASS: fetched offset and value match produced message");

        
        // Verify replication by fetching directly from Broker 2
        System.out.println("\n--- REPLICATION TEST (BROKER 2) ---");

        int previousPort = brokerPort;

        try {
            brokerPort = 9093;

            ByteBuffer replicaResponse = sendRequest(
                Protocol.encodeFetchRequest(topic, 0, offset, 1024 * 1024)
            );

            int replicaCount = replicaResponse.getInt();
            boolean replicaFound = false;

            for (int i = 0; i < replicaCount; i++) {
                long replicaOffset = replicaResponse.getLong();
                int length = replicaResponse.getInt();

                if (length < 0 || length > replicaResponse.remaining()) {
                    throw new IOException("Invalid replica message length");
                }

                byte[] bytes = new byte[length];
                replicaResponse.get(bytes);

                String replicaValue =
                    new String(bytes, StandardCharsets.UTF_8);

                System.out.printf(
                    "BROKER 2 FETCHED: offset=%d, value=%s%n",
                    replicaOffset, replicaValue
                );

                if (replicaOffset == offset && replicaValue.equals(value)) {
                    replicaFound = true;
                }
            }

            if (!replicaFound) {
                throw new IOException(
                    "REPLICATION FAILED: message not found on Broker 2"
                );
            }

            System.out.println(
                "PASS: message replicated successfully to Broker 2"
            );

        } finally {
            brokerPort = previousPort;
        }



        testMetadata();
    }
}
