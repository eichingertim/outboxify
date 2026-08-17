# Outboxify — Spring Kafka (KafkaTemplate Injection) Example

A Spring Boot 4.x microservice demonstrating how **Outboxify** seamlessly binds to custom or existing **Spring `KafkaTemplate`** beans without requiring duplicate broker configuration.

---

## Features Demonstrated

1. **Zero-Configuration KafkaTemplate Auto-Wiring**:
   - Outboxify automatically detects standard Spring `KafkaTemplate` beans (configured via `spring.kafka.*` or custom `@Bean`), eliminating redundant bootstrap broker definitions.
2. **Multi-Pipeline Template Routing**:
   - The `payments` pipeline automatically routes through the default primary `KafkaTemplate`.
   - The `high_priority_payments` pipeline routes through a dedicated named `@Bean("highPriorityKafkaTemplate")` using `outboxify.pipelines.high_priority_payments.broker.kafka-template-ref`.
3. **Transactional Fast-Path Dual-Write**:
   - Updates `PAYMENTS` table and writes outbox rows atomically within a single Spring `@Transactional` boundary.
   - Publishes outbox events via Spring's `KafkaTemplate` only *after* transaction commit.
4. **Atomic Rollback Guarantee**:
   - If an exception occurs before commit, database records roll back and 0 events are sent to Kafka.

---

## Quick Start (30 Seconds)

### 1. Build and Run
From the repository root or the example directory:

```bash
cd examples/java/spring-kafka-example
mvn spring-boot:run
```

The service will start on `http://localhost:8081` with an in-memory H2 database.

---

## Interactive API Tour

### 1. Standard Payment (Routes via Primary `KafkaTemplate`)
```bash
curl -X POST http://localhost:8081/api/payments \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-201", "amount": 89.50, "currency": "USD", "highPriority": false}'
```

**Expected Response**:
```json
{
  "paymentId": "pay-a1b2c3d4",
  "customerId": "cust-201",
  "amount": 89.50,
  "currency": "USD",
  "pipeline": "payments",
  "status": "SETTLED",
  "outboxRecordId": "9b3fa47d-...",
  "createdAt": "2026-08-17T20:45:00Z"
}
```

### 2. High-Priority Payment (Routes via `highPriorityKafkaTemplate`)
```bash
curl -X POST http://localhost:8081/api/payments \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-vip-99", "amount": 50000.00, "currency": "EUR", "highPriority": true}'
```

### 3. Verify Dispatched Kafka Messages
Check which `KafkaTemplate` was used for each message:

```bash
curl http://localhost:8081/api/broker/messages
```

**Response**:
```json
[
  {
    "templateName": "primary-kafka-template",
    "topic": "payments.standard.v1",
    "key": "cust-201",
    "payload": "{\"paymentId\":\"pay-a1b2c3d4\",\"customerId\":\"cust-201\",\"amount\":89.50,\"currency\":\"USD\",\"priority\":\"STANDARD\"}",
    "headers": {
      "eventType": "PaymentSettled",
      "pipeline": "payments"
    }
  },
  {
    "templateName": "high-priority-kafka-template",
    "topic": "payments.vip.v1",
    "key": "cust-vip-99",
    "payload": "{\"paymentId\":\"pay-e5f6g7h8\",\"customerId\":\"cust-vip-99\",\"amount\":50000.00,\"currency\":\"EUR\",\"priority\":\"HIGH\"}",
    "headers": {
      "eventType": "PaymentSettled",
      "pipeline": "high_priority_payments"
    }
  }
]
```

### 4. Verify Rollback Safety
Simulate an error during transaction processing:

```bash
curl -X POST http://localhost:8081/api/payments/simulate-failure \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-failed", "amount": 100.00, "currency": "USD", "highPriority": false}'
```

Check `GET /api/payments` and `GET /api/broker/messages` to verify that 0 records were saved and 0 messages were published.

---

## Embedded Database Console
- **URL**: `http://localhost:8081/h2-console`
- **JDBC URL**: `jdbc:h2:mem:payments_db`
- **Username**: `sa`
- **Password**: *(blank)*
