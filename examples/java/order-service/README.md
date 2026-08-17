# Outboxify — Java Spring Boot Microservice Example

A lightweight Spring Boot 4.x microservice demonstrating the **Transactional Outbox Pattern** with **Outboxify**.

---

## Features Demonstrated

- **Dual-Write Consistency**: Saves domain entity in `ORDERS` table and stages event in `ORDERS_OUTBOX` in the same database transaction.
- **Fast-Path Post-Commit Dispatch**: Dispatches the event to the event broker immediately after the database transaction commits with sub-millisecond latency.
- **Rollback Safety**: If any error occurs before commit, both the order and outbox record roll back atomically; no orphaned messages reach the broker.
- **Watchdog Reaper & Poller**: Background daemon recovers any uncommitted/stalled outbox rows automatically.

---

## Quick Start (30 Seconds)

### 1. Build and Run
From the repository root or the example folder:

```bash
cd examples/java/order-service
mvn spring-boot:run
```

The service will start on `http://localhost:8080` with an in-memory H2 database.

---

## Interactive API Demo

### 1. Create an Order (Success Flow)
```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-101", "item": "Mechanical Keyboard", "amount": 149.99}'
```

**Expected Response**:
```json
{
  "orderId": "ord-7a1b2c3d",
  "customerId": "cust-101",
  "item": "Mechanical Keyboard",
  "amount": 149.99,
  "status": "CONFIRMED",
  "outboxRecordId": "b4e88e1a-...",
  "createdAt": "2026-08-17T20:30:00Z"
}
```

### 2. Check Orders & Outbox Tables
```bash
# View Confirmed Orders
curl http://localhost:8080/api/orders

# View Outbox Table State (Status will be SENT)
curl http://localhost:8080/api/outbox

# View Broker Messages Received
curl http://localhost:8080/api/broker/messages
```

### 3. Simulate Failure / Rollback Safety
```bash
curl -X POST http://localhost:8080/api/orders/simulate-failure \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-102", "item": "Noise-Cancelling Headphones", "amount": 299.99}'
```

**Result**: Transaction is rolled back. Check `GET /api/orders` and `GET /api/broker/messages` to verify that 0 records were saved and 0 messages reached the broker!

---

## Embedded H2 Console
Access the live database UI at:
- **URL**: `http://localhost:8080/h2-console`
- **JDBC URL**: `jdbc:h2:mem:orders_db`
- **Username**: `sa`
- **Password**: *(leave blank)*
