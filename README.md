# Mera-Kafka

Mera-Kafka is an educational, Java-based message-broker project inspired by Apache Kafka. It demonstrates the core ideas behind a broker, append-only message storage, producer/consumer communication, and a small multi-broker cluster coordinated through Apache ZooKeeper.

> **Project status:** Learning project / Kafka-inspired implementation. It is not a drop-in replacement for Apache Kafka and does not yet provide all of Kafka's production durability, consistency, security, or operational guarantees.

## Features

- **Broker server:** Accepts client requests over a TCP connection.
- **Produce and fetch:** Write messages to a topic and fetch them by offset.
- **Append-only storage:** Messages are persisted locally with sequential offsets.
- **Segment and index support:** Storage uses log segments and index files to support message lookup.
- **Recovery:** Persisted logs can be read again after a broker restart.
- **Topics and metadata:** Topic information and replica/leader metadata are coordinated using ZooKeeper.
- **Replication:** A leader sends records to configured replicas.
- **Replica catch-up:** A replica that was unavailable can catch up from the current leader when it returns.
- **Leader failover:** The cluster can select a live replica when the current leader is detected as unavailable.
- **Basic testing utilities:** The project includes a broker test client for produce/fetch and replication checks.

## Technology stack

- Java (Maven compiler release configured to Java 17)
- Maven
- Apache ZooKeeper 3.8.1
- TCP sockets and a project-specific binary protocol
- Local file-based message storage

## Architecture overview

```text
                 +----------------------+
                 |    Test Client       |
                 |  Produce / Fetch     |
                 +-----------+----------+
                             |
                       TCP connection
                             |
                +------------v-----------+
                |       Broker 1         |
                | Storage + Replication  |
                +------------+-----------+
                             |
                         Replication
                             |
                +------------v-----------+
                |       Broker 2         |
                | Storage + Catch-up     |
                +------------------------+

                +------------------------+
                |       ZooKeeper        |
                | Broker / leader        |
                | metadata coordination  |
                +------------------------+
```

The broker stores message data locally. ZooKeeper is used for cluster coordination and leader/replica metadata; it is not the message store.

### Interactive architecture diagram

Once GitHub Pages is enabled for this repository, the interactive diagram will be available at [Mera-Kafka Architecture](https://abhimittal1062.github.io/MeraKafka/). Configure Pages to deploy from the `main` branch and `/docs` folder.

For a local copy, open [`docs/index.html`](docs/index.html) in a browser.

## Prerequisites

Install or make available:

- JDK 17 or newer (the Maven project targets Java 17)
- Maven 3.x
- Docker, if you want to run ZooKeeper in a container

Verify the tools from PowerShell:

```powershell
java -version
mvn -version
docker --version
```

## Getting started

### 1. Start ZooKeeper

If the existing `mera-zookeeper` container is already running, you can reuse it. Otherwise, create a ZooKeeper 3.8.1 container:

```powershell
docker run -d --name mera-zookeeper -p 2181:2181 zookeeper:3.8.1
```

Check the container:

```powershell
docker ps
Test-NetConnection localhost -Port 2181
```

If a container named `mera-zookeeper` already exists but is stopped, start it instead:

```powershell
docker start mera-zookeeper
```

### 2. Open the project directory

```powershell
cd D:\Projects\LEARNINGS\Mera-Kafka
```

### 3. Build the project

```powershell
mvn clean package
```

The project uses Maven and has Java compiler release 17 configured in its `pom.xml`.

## Run the broker

Open a separate PowerShell terminal for each broker and leave each process running.

### Broker 1 — port 9092

```powershell
mvn exec:java "-Dexec.mainClass=com.merakafka.broker.BrokerServer"
```

### Broker 2 — port 9093

```powershell
mvn exec:java "-Dexec.mainClass=com.merakafka.broker.BrokerServer" "-Dexec.args=2 localhost 9093 localhost:2181"
```

Start ZooKeeper before starting the brokers. Broker 1 uses the application's default configuration; Broker 2 is explicitly configured with broker ID `2`, port `9093`, and ZooKeeper at `localhost:2181`.

## Run the test client

With the broker processes running, open another PowerShell terminal in the project directory.

### Basic produce/fetch test against Broker 1

```powershell
mvn exec:java "-Dexec.mainClass=com.merakafka.BrokerTestClient" "-Dexec.classpathScope=test" "-Dexec.args=9092"
```

### Produce/fetch and replication test against Broker 2

```powershell
mvn exec:java "-Dexec.mainClass=com.merakafka.BrokerTestClient" "-Dexec.classpathScope=test" "-Dexec.args=9093 replication-v2"
```

The test client output should be checked for successful produce/fetch results and, when both brokers are running and configured for the topic, successful replication checks. A replication test may fail if the replica it attempts to contact is offline; that does not by itself mean the active broker failed to produce or fetch a message.

> If your `BrokerTestClient` has been changed to require additional arguments, use the argument format defined in its current source file.

## Data directory

Runtime data is written under `data/`, including broker-specific data and topic data. Depending on the current implementation, you may see log segments and index files there.

**Do not delete `data/` if you want to keep your local test messages.** For version control, generated runtime data should generally be excluded from Git; add `data/` to `.gitignore`.

## Git setup

A suggested `.gitignore` for this project:

```gitignore
target/
*.class
.idea/
.vscode/
data/
```

Stage source and project configuration rather than generated runtime data:

```powershell
git add .gitignore pom.xml src/
git status
git commit -m "Implement broker replication, failover and replica catch-up"
```

Review `git status` before committing to ensure that runtime data and build output are not staged.

## Current implementation notes and limitations

- This is an educational implementation with a project-specific protocol, not the Apache Kafka wire protocol.
- Replication and leader failover have been exercised in local tests, including replica catch-up after restart.
- A successful local test does not establish zero data loss under every crash timing or network-failure scenario.
- The current implementation may accept a write while a replica is unavailable and rely on catch-up when that replica returns. Do not assume Kafka-style `acks`, in-sync replica, or exactly-once guarantees unless those behaviours are explicitly implemented and tested.
- Security, authentication, authorization, production monitoring, and full Kafka compatibility are outside the current scope.

## Suggested next steps

1. Define explicit producer acknowledgement and replication policies.
2. Track replica progress and in-sync replicas.
3. Add tests for leader crashes during an in-flight produce request.
4. Improve protocol error handling and client timeouts.
5. Add automated integration tests for restart, catch-up, and failover scenarios.

## License

No license has been specified yet. Add a `LICENSE` file before presenting the project as open source or allowing others to reuse it.
