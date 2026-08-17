# Outboxify — Python FastAPI Microservice Example

A FastAPI and SQLAlchemy microservice demonstrating the **Transactional Outbox Pattern** using `outboxify`.

---

## Features Demonstrated

- **Dual-Write Consistency**: Persists order entity to SQLite and registers the outbox event within the same database transaction.
- **Fast-Path Post-Commit Dispatch**: Dispatches the event via `SqlAlchemySessionHookManager` immediately upon transaction commit with zero polling latency.
- **Rollback Safety**: If an exception occurs before commit, the session rolls back and Outboxify drops staged records, guaranteeing zero phantom events reach Kafka.
- **Lifecycle Management**: `OutboxifyAppManager` integrates natively with FastAPI's `lifespan` handler to manage background poller and watchdog reaper daemons.

---

## Quick Start (15 Seconds)

### 1. Install & Run
```bash
cd examples/python/order-service

# Install dependencies (or activate your virtualenv)
pip install -e .

# Run with Uvicorn
uvicorn app.main:app --reload --port 8000
```

The service will start on `http://localhost:8000` with Swagger UI at `http://localhost:8000/docs`.

---

## Interactive API Demo

### 1. Create an Order (Success Flow)
```bash
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "cust-301", "item": "Mechanical Gaming Keyboard", "amount": 179.99}'
```

**Expected Response**:
```json
{
  "id": "ord-4a2b1c8f",
  "customer_id": "cust-301",
  "item": "Mechanical Gaming Keyboard",
  "amount": 179.99,
  "status": "CONFIRMED",
  "created_at": "2026-08-17T20:35:00.000000Z"
}
```

### 2. Inspect Outbox and Broker State
```bash
# View Confirmed Orders
curl http://localhost:8000/api/orders

# View Outbox Table State (Status will be SENT)
curl http://localhost:8000/api/outbox

# View Broker Messages Delivered
curl http://localhost:8000/api/broker/messages
```

### 3. Simulate Failure / Rollback Safety
```bash
curl -X POST http://localhost:8000/api/orders/simulate-failure \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "cust-302", "item": "Smart Speaker", "amount": 99.99}'
```

**Result**: The transaction is rolled back cleanly. Verify with `GET /api/orders` and `GET /api/broker/messages` that no phantom messages were sent!
