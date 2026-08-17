# Outboxify — Node.js Microservice Example

A Node.js / Express microservice demonstrating the **Transactional Outbox Pattern** using `@outboxify/core`.

---

## Features Demonstrated

- **Dual-Write Consistency**: Stages domain orders and outbox records atomically.
- **Fast-Path Post-Commit Dispatch**: Dispatches events to the message broker immediately upon transaction commit with zero polling lag.
- **Rollback Safety**: Any unexpected failure rolls back the transaction before commit, guaranteeing that zero orphaned events reach the broker.
- **Background Daemon Engine**: Built-in `OutboxPollerEngine` and `OutboxReaperJob` automatically recover and retry stalled records.

---

## Quick Start (15 Seconds)

### 1. Install & Run
```bash
cd examples/node/order-service
npm install
npm start
```

The service will start on `http://localhost:3001`.

---

## Interactive API Demo

### 1. Create an Order (Success Flow)
```bash
curl -X POST http://localhost:3001/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-201", "item": "4K Ultra HD Monitor", "amount": 349.99}'
```

**Expected Response**:
```json
{
  "order": {
    "id": "ord-8f2e1a90",
    "customerId": "cust-201",
    "item": "4K Ultra HD Monitor",
    "amount": 349.99,
    "status": "CONFIRMED",
    "createdAt": "2026-08-17T20:34:00.000Z"
  },
  "outboxRecordId": "3b7a1234-..."
}
```

### 2. Inspect Outbox and Broker State
```bash
# View Confirmed Orders
curl http://localhost:3001/api/orders

# View Outbox Table State (Status will be SENT)
curl http://localhost:3001/api/outbox

# View Broker Messages Delivered
curl http://localhost:3001/api/broker/messages
```

### 3. Simulate Failure / Rollback Safety
```bash
curl -X POST http://localhost:3001/api/orders/simulate-failure \
  -H "Content-Type: application/json" \
  -d '{"customerId": "cust-202", "item": "Ergonomic Chair", "amount": 249.99}'
```

**Result**: The transaction is rolled back. Check `GET /api/orders` and `GET /api/broker/messages` to verify that 0 records and 0 messages were published!
