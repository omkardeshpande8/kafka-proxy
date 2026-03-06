# Kafka Proxy Implementation Plan

## Goal Description
Create a flexible, high-performance TCP proxy in Java using Netty. This proxy will sit between Kafka clients and brokers, allowing extending functionality (interception, logging, additional protocol support) in the future.
Initially, it will function as a transparent TCP proxy.

## User Review Required
> [!IMPORTANT]
> **Proxy Type**: This plan builds a **TCP level** proxy (Layer 4). It forwards bytes without strict protocol parsing.
> IF you need a **REST Proxy** (HTTP -> Kafka) or a **Protocol-Aware Proxy** (modifying Kafka messages in flight), please specify!

## Proposed Changes

### dependencies
#### [MODIFY] [pom.xml](file:///home/omkar/.gemini/antigravity/scratch/hello-world/pom.xml)
- Add `netty-all` dependency.
- Add `kafka-clients` and `kafka_2.13` (or similar) + `spring-kafka-test` (or `kafka-junit`) for EmbeddedKafka testing.
- Upgrade compiler to Java 11 or 17 if possible (currently 1.8), but will stick to 1.8 if restricted.

### Source Code
#### [NEW] [KafkaProxy.java](file:///home/omkar/.gemini/antigravity/scratch/hello-world/src/main/java/com/example/proxy/KafkaProxy.java)
- Main class bootstrapping the Netty ServerBootstrap.

#### [NEW] [ProxyFrontendHandler.java](file:///home/omkar/.gemini/antigravity/scratch/hello-world/src/main/java/com/example/proxy/ProxyFrontendHandler.java)
- Handles incoming connections from clients.
- Initiates connection to the Backend Kafka broker.

#### [NEW] [ProxyBackendHandler.java](file:///home/omkar/.gemini/antigravity/scratch/hello-world/src/main/java/com/example/proxy/ProxyBackendHandler.java)
- Handles traffic from the backend broker and forwards it back to the client.

## Verification Plan

### Automated Tests
Run `mvn test`.
I will add an integration test that:
1.  Starts a simple TCP Echo Server (simulating Kafka for connectivity test) OR an Embedded Kafka.
2.  Starts the Proxy.
3.  Sends data to Proxy -> Verifies it reaches the "Broker".
4.  Verifies response from "Broker" -> Proxy -> Client.

### Manual Verification
(Since Docker is unavailable, manual verification with a real Kafka cluster might be hard unless the user provides one).
- I will rely heavily on the integration test.
