package com.merakafka.broker;

public class BrokerInfo {
    private final int brokerId;
    private final String host;
    private final int port;

    public BrokerInfo(int brokerId, String host, int port) {
        if (brokerId < 0) {
            throw new IllegalArgumentException("Broker ID cant be negative");
        }

        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Broker host cannot be empty");
        }

        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Broker Port is Invalid");
        }

        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "BrokerInfo{" + "brokerId=\'" + brokerId + "\'" + ",host=\'" + host + "\'" + ",port=\'" + port + '}';
    }
}