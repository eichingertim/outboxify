# Outboxify — Polyglot Microservice Examples

Welcome to the **Outboxify** example microservices showcase. This directory provides three lightweight, standalone microservices demonstrating the **Transactional Outbox Pattern** across Java, Node.js, and Python.

---

## Polyglot Microservice Demos

| Language / Framework | Directory | Default Port | Storage & Broker |
| :--- | :--- | :--- | :--- |
| **Java (Spring Boot 4.x)** | [`examples/java/order-service`](file:///Users/timeichinger/coding-stuff/outboxify/examples/java/order-service) | `8080` | In-Memory H2 + Embedded / Live Kafka |
| **Node.js (Express / Core)** | [`examples/node/order-service`](file:///Users/timeichinger/coding-stuff/outboxify/examples/node/order-service) | `3001` | In-Memory SQLite + Embedded / Live Kafka |
| **Python (FastAPI / SQLAlchemy)** | [`examples/python/order-service`](file:///Users/timeichinger/coding-stuff/outboxify/examples/python/order-service) | `8000` | SQLite DB (`orders.db`) + Embedded / Live Kafka |

---

## Key Patterns Demonstrated in Every Example

1. **Dual-Write Consistency**: Persisting a business entity (e.g. Order) and its outbox event atomically in a single local database transaction.
2. **Fast-Path Post-Commit Dispatch**: Publishing the event to the broker immediately *after* database commit with sub-millisecond latency.
3. **Rollback & Poison-Pill Safety**: If an error occurs before commit, the transaction rolls back cleanly and Outboxify guarantees **zero phantom/orphaned messages** reach the event broker.
4. **Watchdog Reaper & Polling Daemon**: Background workers continually recover stalled records and guarantee **at-least-once delivery**.

---

## 30-Second Quick Start

### 1. Java (Spring Boot 4.x)
```bash
cd examples/java/order-service
mvn spring-boot:run
```
- **Test Endpoint**: `POST http://localhost:8080/api/orders`
- **H2 Console**: `http://localhost:8080/h2-console`

---

### 2. Node.js
```bash
cd examples/node/order-service
npm install
npm start
```
- **Test Endpoint**: `POST http://localhost:3001/api/orders`

---

### 3. Python (FastAPI)
```bash
cd examples/python/order-service
pip install -e .
uvicorn app.main:app --reload --port 8000
```
- **Test Endpoint**: `POST http://localhost:8000/api/orders`
- **Swagger Docs**: `http://localhost:8000/docs`

---

## Optional: Running with Live PostgreSQL & Kafka

All examples run out of the box with zero external dependencies using embedded/in-memory components. If you want to connect them to live PostgreSQL and Kafka instances:

```bash
docker compose -f examples/docker-compose.yml up -d
```
