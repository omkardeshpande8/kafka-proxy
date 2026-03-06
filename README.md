# Kafka Proxy (Conduktor Gateway Parity)

A high-performance, protocol-aware Kafka proxy built with Netty, designed to provide features similar to Conduktor Gateway.

## Features

### 1. Kafka Protocol Awareness (Layer 7)
The proxy understands the Kafka protocol, allowing it to inspect and modify messages in flight. It handles message framing and decodes request headers (API Key, Version, Correlation ID, Client ID).

### 2. Interceptor Framework
A flexible framework for adding custom logic to the request/response pipeline. Interceptors can be easily plugged in and configured via `proxy.properties`.

### 3. Audit Logging
Every Kafka request is logged with metadata, providing a clear audit trail of client activity.
- **Log format**: `[AUDIT] Request from <address>: API=<Name>(<ID>), Version=<V>, CorrelationId=<ID>, ClientId=<ID>`

### 4. Topic Guardrails
Protect your Kafka cluster by enforcing policies on topic access.
- **Regex-based blocking**: Prevent `PRODUCE` or `FETCH` requests to sensitive topics.

### 5. Topic Aliasing (Virtual Clusters)
Decouple logical topic names used by clients from physical topic names in the cluster.
- **Remapping**: Transparently map virtual topics to physical topics in Produce and Fetch requests.

### 6. Chaos Engineering
Test application resilience by injecting artificial failures.
- **Latency Injection**: Add configurable delays to Kafka requests.
- **Failure Simulation**: Randomly drop connections to simulate broker instability.

### 7. Producer Rate Limiting
Prevent single tenants from overwhelming the cluster.
- **Throttling**: Limit the bytes-per-second rate for Produce requests.

### 8. Data Quality (JSON Validation)
Ensure data integrity by validating message payloads at the source.
- **Schema Validation**: Block Produce requests with invalid JSON payloads.

## Configuration

Configuration is managed via the `proxy.properties` file.

```properties
# Enable Audit Logging
interceptor.audit.enabled=true

# Enable Topic Guardrails
interceptor.guardrail.blocked_topics=forbidden-topic,restricted-.*

# Enable Topic Aliasing
interceptor.alias.virtual-topic=physical-topic

# Enable Chaos Engineering
interceptor.chaos.latency=500
interceptor.chaos.error_rate=0.1

# Enable Rate Limiting
interceptor.ratelimit.max_bps=1048576

# Enable Data Quality
interceptor.dataquality.json_validation=true
```

## Getting Started

### Prerequisites
- Java 8 or higher
- Maven

### Build
```bash
mvn clean compile
```

### Run
```bash
mvn exec:java -Dexec.mainClass="com.example.proxy.KafkaProxy"
```

### Running Tests
```bash
mvn test
```
