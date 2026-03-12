# Kafka Proxy (Conduktor Gateway Parity)

A high-performance, protocol-aware Kafka proxy built with Netty, designed to provide features similar to Conduktor Gateway.

## Features

### 1. Kafka Protocol Awareness (Layer 7)
The proxy understands the Kafka protocol, allowing it to inspect and modify messages in flight. It handles message framing and decodes request headers (API Key, Version, Correlation ID, Client ID) using official `kafka-clients` utilities.

### 2. Async Interceptor Framework
A flexible, non-blocking framework for adding custom logic to the request/response pipeline. Interceptors are executed asynchronously to ensure high performance and prevent EventLoop stalling.

### 3. Audit Logging
Every Kafka request is logged with metadata, providing a clear audit trail of client activity.
- **Log format**: `[AUDIT] Request from <address>: API=<Name>(<ID>), Version=<V>, CorrelationId=<ID>, ClientId=<ID>`

### 4. Topic Guardrails & Aliasing
- **Security Guardrails**: Prevent `PRODUCE` or `FETCH` requests to sensitive topics using regex patterns.
- **Virtual Topics (Aliasing)**: Transparently map virtual topics used by clients to physical topics in the cluster. (Note: Same-length names required for POC in-place replacement).

### 5. Chaos Engineering
Test application resilience by injecting artificial failures.
- **Latency Injection**: Add configurable delays to Kafka requests.
- **Failure Simulation**: Randomly drop connections to simulate broker instability.

### 6. Producer Rate Limiting
Eliminate "noisy neighbors" by enforcing bandwidth limits per tenant or cluster.
- **Throttling**: Limit the bytes-per-second rate for Produce requests using non-blocking delays.

### 7. Data Quality & Privacy
- **JSON Validation**: Ensure data integrity by validating message payloads at the source.
- **PII Masking**: Protect sensitive data by masking specific fields in JSON payloads (e.g., SSN, Credit Card numbers).

### 8. Enterprise Resilience
- **Cluster Failover**: Seamlessly switch between primary and secondary Kafka clusters without client configuration changes.
- **Large Payload Offloading**: Automatically offload oversized messages to external storage (e.g., local disk) to prevent broker performance degradation.
- **Fetch Caching**: Reduce broker load by serving repeated Fetch requests directly from the proxy's cache.

## Detailed Architecture Documentation

For a full system design and implementation deep-dive (including architecture and sequence diagrams, component responsibilities, interceptor behavior, and operational constraints), see:

- [`docs/architecture.md`](docs/architecture.md)

## Configuration

Configuration is managed via the `proxy.properties` file.

```properties
# --- Audit Logging ---
interceptor.audit.enabled=true

# --- Security Guardrails ---
interceptor.guardrail.blocked_topics=forbidden-topic,restricted-.*

# --- Topic Aliasing ---
interceptor.alias.virtual=virtual-topic
interceptor.alias.physical=physical-topi

# --- Chaos Engineering ---
interceptor.chaos.latency=500
interceptor.chaos.error_rate=0.1

# --- Rate Limiting ---
interceptor.ratelimit.max_bps=1048576

# --- Data Quality (JSON Validation) ---
interceptor.dataquality.json_validation=true

# --- PII Masking ---
interceptor.masking.fields=ssn,credit_card

# --- Large Payload Offloading ---
interceptor.offload.threshold_bytes=1048576

# --- Fetch Caching ---
interceptor.cache.enabled=true

# --- Multi-Region Routing & Selective Failover ---
routing.backends=region-a,region-b
routing.backend.region-a.host=kafka-a.internal
routing.backend.region-a.port=9092
routing.backend.region-b.host=kafka-b.internal
routing.backend.region-b.port=9092
routing.default_backend=region-a
routing.topic_routes=payments-.*->region-b,critical-.*->region-b
```


### Multi-Region (Stretch) Deployment + Selective Topic Failover
Deploy multiple proxy instances across regions behind a global load balancer (active/active or active/passive).
Each proxy can route topic traffic to different Kafka backends based on regex patterns:

- Define regional backends with `routing.backend.<name>.host` and `.port`.
- Choose a `routing.default_backend` for normal traffic.
- Override specific topic families with `routing.topic_routes` (regex -> backend).

This enables per-topic failover without changing client bootstrap settings.

Connection behavior: each client TCP connection is pinned to a single resolved backend region after its first routable request. If later requests on that same connection would resolve to a different region, the proxy closes the connection to avoid cross-region mixing.

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

### Scale Correctness + Performance Suite
Run a dedicated load suite that validates message integrity under concurrency and reports latency/throughput metrics.

```bash
./scripts/run-perf-tests.sh
```

Useful overrides for larger runs:

```bash
CORRECTNESS_THREADS=24 \
CORRECTNESS_MESSAGES=2000 \
PERF_THREADS=32 \
PERF_MESSAGES=4000 \
PERF_PAYLOAD_BYTES=1024 \
PERF_MIN_THROUGHPUT=5000 \
PERF_MAX_P95_MS=20 \
./scripts/run-perf-tests.sh
```

