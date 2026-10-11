package com.merakafka.client;

import com.merakafka.broker.Protocol;
import com.merakafka.Message;

import java.util.ArrayList;
import java.util.List;
import java.time.Instant;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class KafkaClient {
    private final String host;
    private final int port;

    public KafkaClient(String host,int port) {
        this.host=host;
        this.port=port;
    }

    private ByteBuffer sendRequest(ByteBuffer request) throws IOException {
        try(Socket socket=new Socket(host,port)) {
            socket.getOutputStream().write(request.array(),request.position(),request.remaining());
            socket.getOutputStream().flush();

            DataInputStream in=new DataInputStream(socket.getInputStream());

            int responseType=in.readUnsignedByte();

            if(responseType==Protocol.ERROR_RESPONSE) {
                int length=in.readShort();

                if(length<0) {
                    throw new IOException("Invalid error response length");
                }

                byte[] error=new byte[length];

                in.readFully(error);

                throw new IOException(new String(error,StandardCharsets.UTF_8));
            }

            if(responseType==Protocol.PRODUCE_RESPONSE) {
                long offset=in.readLong();
                byte status=in.readByte();

                ByteBuffer response=ByteBuffer.allocate(Long.BYTES+1);
                response.putLong(offset).put(status).flip();
                return response;
            }

            if(responseType==Protocol.FETCH_RESPONSE) {
                int count=in.readInt();

                if(count<0) {
                    throw new IOException("Invalid fetch message count");
                }

                java.io.ByteArrayOutputStream bytes=new java.io.ByteArrayOutputStream();

                try(java.io.DataOutputStream out=new java.io.DataOutputStream(bytes)) {
                    out.writeInt(count);

                    for(int i=0;i<count;i++) {
                        long offset=in.readLong();
                        int length=in.readInt();

                        if(length<0) {
                            throw new IOException("Invalid message length");
                        }

                        byte[] value=new byte[length];
                        in.readFully(value);

                        out.writeLong(offset);
                        out.writeInt(length);
                        out.write(value);
                    }
                }

                return ByteBuffer.wrap(bytes.toByteArray());
            }

            throw new IOException("Unexpected response type: "+responseType);
        }
    }

    public long produce(String topic,String value) throws IOException {
        ByteBuffer response=sendRequest(Protocol.encodeProduceRequest(topic, 0, value.getBytes(StandardCharsets.UTF_8)));
        long offset=response.getLong();
        byte status=response.get();

        if(status!=0) {
            throw new IOException("Produce Failed with Status: "+status);
        }

        return offset;
    }

    public List<Message> fetch(String topic, long offset,int maxBytes) throws IOException {
        ByteBuffer response=sendRequest(Protocol.encodeFetchRequest(topic, 0, offset, maxBytes));
        int count=response.getInt();

        if(count<0) {
            throw new IOException("Invalid fetch message count: "+count);
        }

        List<Message> messages=new ArrayList<>();

        for(int i=0;i<count;i++)  {
            long fetchedOffset=response.getLong();
            int length=response.getInt();

            if(length<0 || length>response.remaining()) {
                throw new IOException("Invalid fetch message length: "+length);
            }

            byte[] valueBytes=new byte[length];

            response.get(valueBytes);

            String value=new String(valueBytes,StandardCharsets.UTF_8);
            Message message=new Message(topic, value);
            message.setOffset(fetchedOffset);
            messages.add(message);
        }

        return messages;
    }
}
