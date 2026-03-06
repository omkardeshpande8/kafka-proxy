# Kafka Proxy Walkthrough

I have successfully implemented a basis **Netty-based TCP Proxy** that can forward traffic between a generic client and a server (simulating a Kafka broker).

## Changes

### TCP Proxy Implementation
The proxy consists of three main classes in `src/main/java/com/example/proxy/`:
1.  **[KafkaProxy.java](file:///home/omkar/.gemini/antigravity/scratch/hello-world/src/main/java/com/example/proxy/KafkaProxy.java)**: The main entry point. It sets up the Netty `ServerBootstrap`.
2.  **[ProxyFrontendHandler.java](file:///home/omkar/.gemini/antigravity/scratch/hello-world/src/main/java/com/example/proxy/ProxyFrontendHandler.java)**: Handles incoming client connections. When a client connects, it initiates a connection to the backend target.
3.  **[ProxyBackendHandler.java](file:///home/omkar/.gemini/antigravity/scratch/hello-world/src/main/java/com/example/proxy/ProxyBackendHandler.java)**: Handles the connection to the backend. It reads data from the backend and writes it back to the client.

### Dependencies
Updated **[pom.xml](file:///home/omkar/.gemini/antigravity/scratch/hello-world/pom.xml)** to include:
- `netty-all` (4.1.101.Final)
- `kafka-clients`
- `junit` (4.13.2)

## Verification Results

### Automated Tests
I created an integration test **[KafkaProxyTest.java](file:///home/omkar/.gemini/antigravity/scratch/hello-world/src/test/java/com/example/proxy/KafkaProxyTest.java)** that:
1.  Starts a temporary Echo Server in a separate thread (acting as the backend).
2.  Starts the `KafkaProxy` forwarding to that Echo Server.
3.  Connects a client to the Proxy.
4.  Sends a message and verifies it receives the exact same message back.

**Test Result**:
```
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

The proxy is now ready for further enhancements, such as parsing the Kafka protocol bytes to inspect or modify messages.
