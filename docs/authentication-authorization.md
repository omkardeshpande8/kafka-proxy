# Authentication and Authorization

This proxy supports Kafka authentication/authorization in two complementary ways:

1. **Protocol pass-through** to backend Kafka brokers (recommended baseline).
2. **Optional proxy-side enforcement** for additional controls.

## Authentication support

Because the proxy forwards Kafka request/response payload bytes unchanged, it is compatible with all broker-side SASL mechanisms and security protocols that Kafka clients and brokers negotiate:

- `PLAINTEXT`
- `SSL`
- `SASL_PLAINTEXT`
- `SASL_SSL`

### SASL mechanisms

The proxy is mechanism-agnostic and transparently forwards:

- `PLAIN`
- `SCRAM-SHA-256`
- `SCRAM-SHA-512`
- `GSSAPI`
- `OAUTHBEARER`
- Any custom mechanism your Kafka distribution supports.

## TLS termination / re-encryption

You can independently enable TLS on:

- **Frontend** (client -> proxy)
- **Backend** (proxy -> broker)

This supports common topologies such as:

- TLS termination at proxy (`SSL` clients, plaintext backend)
- End-to-end encryption with re-encryption (`SSL`/`SASL_SSL` on both sides)
- mTLS from clients to proxy and/or from proxy to brokers.

### Frontend TLS properties

- `security.frontend.tls.enabled`
- `security.frontend.tls.keystore.path`
- `security.frontend.tls.keystore.password`
- `security.frontend.tls.keystore.type`
- `security.frontend.tls.mtls.required`
- `security.frontend.tls.truststore.path`
- `security.frontend.tls.truststore.password`
- `security.frontend.tls.truststore.type`

### Backend TLS properties

- `security.backend.tls.enabled`
- `security.backend.tls.truststore.path`
- `security.backend.tls.truststore.password`
- `security.backend.tls.truststore.type`
- `security.backend.tls.client_auth.enabled`
- `security.backend.tls.keystore.path`
- `security.backend.tls.keystore.password`
- `security.backend.tls.keystore.type`

## Authorization support

### 1) Backend/native Kafka authorization

Your Kafka cluster remains the source of truth for native authorization:

- Apache Kafka ACLs
- Distribution-specific RBAC and custom authorizers

The proxy does not alter broker authorization semantics.

### 2) Proxy-side authorization interceptor (optional)

Enable an extra policy layer with:

- `interceptor.authz.enabled=true`
- `interceptor.authz.default_action=allow|deny`
- `interceptor.authz.rules=<rules>`

#### Rule format

Rules are evaluated in order, first match wins.

```text
<decision>:<predicate>,<predicate>;...
```

- `decision`: `allow` or `deny`
- supported predicates:
  - `api=<KafkaApiName>` (e.g. `PRODUCE`, `FETCH`)
  - `topic=<regex>`
  - `client=<regex>` (alias: `client_id`)
  - `principal=<regex>` (mTLS peer principal DN)

#### Example

```properties
interceptor.authz.enabled=true
interceptor.authz.default_action=deny
interceptor.authz.rules=\
allow:api=PRODUCE,client=payments-.*;\
allow:api=FETCH,client=analytics-.*;\
deny:topic=restricted-.*
```

## Notes and limits

- Principal-based rules require frontend TLS with mTLS enabled.
- Topic matching relies on request decoding for Produce/Fetch topic extraction.
- For comprehensive enterprise policy (including consumer-group/resource-level checks), keep broker-native authorization enabled and use proxy authz as a supplemental guardrail.
