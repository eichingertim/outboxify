# Outboxify

<p align="center">
  <strong>Universal Polyglot Transactional Outbox Engine</strong><br>
  <em>Dual-Write Consistency with Sub-Millisecond Fast-Path Dispatch for Java, TypeScript, and Python.</em>
</p>

<p align="center">
  <a href="https://github.com/eichingertim/outboxify/actions/workflows/ci.yml"><img src="https://github.com/eichingertim/outboxify/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/eichingertim/outboxify/releases"><img src="https://img.shields.io/badge/version-0.1.0-blue.svg" alt="Version 0.1.0"></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://openjdk.org/"><img src="https://img.shields.io/badge/Java-21%2B%20%7C%2025%2B-orange.svg" alt="Java 21+"></a>
  <a href="https://nodejs.org/"><img src="https://img.shields.io/badge/Node.js-20%2B%20%7C%2022%2B%20%7C%2024%2B-green.svg" alt="Node.js 20+"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.11%2B%20%7C%203.12%2B%20%7C%203.13%2B-blue.svg" alt="Python 3.11+"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-4.x-brightgreen.svg" alt="Spring Boot 4.x"></a>
  <a href="https://kafka.apache.org/"><img src="https://img.shields.io/badge/Kafka-4.x%20KRaft-black.svg" alt="Kafka 4.x"></a>
</p>

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture & How It Works](#architecture--how-it-works)
- [Database Dialects Matrix](#database-dialects-matrix)
- [Polyglot Quickstart](#polyglot-quickstart)
  - [Java (Spring Boot 4.x / JDK 21+)](#1-java-spring-boot-4x--jdk-21)
  - [Node.js / TypeScript (NestJS, Prisma, TypeORM)](#2-nodejs--typescript-nestjs-prisma-typeorm)
  - [Python (FastAPI, SQLAlchemy)](#3-python-fastapi-sqlalchemy)
- [Configuration Reference](#configuration-reference)
- [Runnable Examples](#runnable-examples)
- [Building & Testing](#building--testing)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

---

## Overview

When microservices need to modify a database and emit events to a message broker (such as Apache Kafka), doing both in separate network calls creates the classic **dual-write distributed consistency problem**:

- If the database commit succeeds but the message broker publish fails (e.g. network partition), events are lost.
- If the broker publish succeeds before the database commits, downstream consumers process phantom data if the database transaction rolls back.

**Outboxify** solves dual-write consistency by persisting outbox records inside the **same ACID transaction** as your business data, guaranteeing **at-least-once delivery** with **zero dual-write data loss**.

Outboxify is designed from the ground up for modern enterprise workloads:
- **Sub-Millisecond Fast-Path**: Dispatches messages immediately upon transaction commit using post-commit synchronization hooks without waiting for poller intervals.
- **Resilient Slow-Path Poller**: A background poller recovers and dispatches any unprocessed messages after outages or node crashes.
- **Autonomous Watchdog Reaper**: Detects and un-sticks orphaned `PROCESSING` rows exceeding timeouts with exponential backoff and dead-letter handling.
- **Polyglot & Framework-Free Core**: Standardized state machines and JSON schemas with native integrations for Spring Boot, NestJS, Prisma, TypeORM, FastAPI, and SQLAlchemy.

---

## Key Features

- ⚡ **Zero-Latency Post-Commit Hook**: Instant event dispatch upon transaction commit.
- 🛡️ **Guaranteed Rollback Safety**: Events are never dispatched if the transaction aborts.
- 🔒 **Non-Blocking Row Locking**: High-throughput database polling with `FOR UPDATE SKIP LOCKED` / `READPAST`.
- ☕ **Spring KafkaTemplate Auto-Wiring**: Automatically binds to Spring Boot's existing `KafkaTemplate` or routes across named beans per pipeline without duplicate broker definitions.
- 🗄️ **Multi-Database Support**: Oracle, PostgreSQL, MySQL, Microsoft SQL Server, and SQLite.
- 🚦 **Multi-Pipeline Routing**: Configure isolated outbox tables, polling intervals, retry policies, and Kafka broker destinations per event pipeline.
- 📦 **Zero Mandatory Dependencies**: Core libraries rely strictly on clean SPI interfaces (`BrokerPublisher`, `OutboxRepository`, `DatabaseDialect`).

---

## Architecture & How It Works

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant App as Application Service
    participant DB as Relational Database
    participant Hook as Post-Commit Hook
    participant Broker as Apache Kafka
    participant Poller as Background Poller / Reaper

    Client->>App: Business Operation (e.g., Create Order)
    activate App
    App->>DB: BEGIN Transaction
    App->>DB: INSERT / UPDATE Business Entity
    App->>DB: INSERT Outbox Record (Status: PENDING)
    App->>DB: COMMIT Transaction
    deactivate DB
    
    alt Fast-Path (Sub-Millisecond Dispatch)
        DB-->>Hook: Transaction Committed Event
        Hook->>Broker: Async Send (Record Metadata)
        Broker-->>Hook: ACK (Partition, Offset)
        Hook->>DB: UPDATE Outbox Record (Status: PROCESSED)
    else Fallback Slow-Path (Failure / Crash Recovery)
        Poller->>DB: SELECT PENDING / FAILED FOR UPDATE SKIP LOCKED
        Poller->>DB: UPDATE Status -> PROCESSING
        Poller->>Broker: Batch Publish
        Broker-->>Poller: Batch ACK
        Poller->>DB: UPDATE Status -> PROCESSED
    end
    App-->>Client: Success Response
    deactivate App
```

---

## Database Dialects Matrix

Outboxify encapsulates database-specific concurrency control behind a pluggable `DatabaseDialect` SPI:

| Database | Row Locking Strategy | Paging Syntax | High-Precision Timestamp | Sparse Indexing |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** (9.5+) | `FOR UPDATE SKIP LOCKED` | `LIMIT :n` | `CURRENT_TIMESTAMP` | Partial Index (`WHERE status != 'PROCESSED'`) |
| **Oracle** (12c+) | `FOR UPDATE SKIP LOCKED` | `FETCH FIRST :n ROWS ONLY` | `SYSTIMESTAMP` | Function-Based Index |
| **MySQL** (8.0+) | `FOR UPDATE SKIP LOCKED` | `LIMIT :n` | `NOW(6)` | Composite Index |
| **MS SQL Server** (2019+) | `WITH (UPDLOCK, READPAST, ROWLOCK)` | `TOP (:n)` | `SYSUTCDATETIME()` | Filtered Index (`WHERE status != 'PROCESSED'`) |
| **SQLite** (3.35+) | Single-Writer Transaction | `LIMIT :n` | `CURRENT_TIMESTAMP` | Partial Index |

---

## Polyglot Quickstart

### 1. Java (Spring Boot 4.x / JDK 21+)

#### Add Dependency
```xml
<dependency>
    <groupId>io.outboxify</groupId>
    <artifactId>outboxify-spring-boot-starter</artifactId>
    <version>0.1.0</version>
</dependency>
```

#### Application Code
```java
@Service
public class OrderService {

    private final OutboxPublisher outboxPublisher;
    private final OrderRepository orderRepository;

    public OrderService(OutboxPublisher outboxPublisher, OrderRepository orderRepository) {
        this.outboxPublisher = outboxPublisher;
        this.orderRepository = orderRepository;
    }

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        // 1. Persist business entity
        Order order = orderRepository.save(new Order(request.customerId(), request.amount()));

        // 2. Stage outbox record in the same transaction
        outboxPublisher.publish("orders", OutboxPayload.builder()
                .topic("orders.v1")
                .partitionKey(order.getCustomerId())
                .payload("{\"orderId\":\"" + order.getId() + "\",\"amount\":" + order.getAmount() + "}")
                .header("traceId", UUID.randomUUID().toString())
                .header("eventType", "OrderCreated")
                .build());

        return new OrderResponse(order.getId(), "CONFIRMED");
    }
}
```

> **Spring KafkaTemplate Auto-Detection**: If you have `spring-kafka` on your classpath or define a `KafkaTemplate` bean, Outboxify auto-detects it and dispatches records through your existing broker configuration.

---

### 2. Node.js / TypeScript (NestJS, Prisma, TypeORM)

#### Install Package
```bash
npm install @outboxify/core @outboxify/nestjs
# or for Prisma: npm install @outboxify/prisma
# or for TypeORM: npm install @outboxify/typeorm
```

#### NestJS Module Setup
```typescript
import { Module } from '@nestjs/common';
import { OutboxifyModule, PipelineConfig, SqlOutboxRepository, KafkaBrokerPublisher } from '@outboxify/nestjs';

@Module({
  imports: [
    OutboxifyModule.forRoot({
      pipelines: {
        orders: new PipelineConfig({
          name: 'orders',
          tableName: 'ORDERS_OUTBOX',
          batchSize: 100,
          pollIntervalMs: 1000
        }),
      },
      repository: new SqlOutboxRepository(dbPool),
      brokerPublisher: new KafkaBrokerPublisher(kafkaProducer)
    })
  ]
})
export class AppModule {}
```

---

### 3. Python (FastAPI, SQLAlchemy)

#### Install Package
```bash
pip install "outboxify[sqlalchemy,fastapi,kafka]"
```

#### FastAPI Lifespan & Service
```python
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from outboxify import OutboxPublisher, OutboxPayload, PipelineConfig, SqlAlchemyOutboxRepository, KafkaBrokerPublisher
from outboxify.fastapi import create_outboxify_lifespan, OutboxifyAppManager

pipelines = {"orders": PipelineConfig(name="orders", table_name="orders_outbox")}
app_manager = OutboxifyAppManager(pipelines, repo, broker)

app = FastAPI(lifespan=create_outboxify_lifespan(app_manager))

@app.post("/api/orders")
async def create_order(request: OrderCreate, db: Session = Depends(get_db)):
    # 1. Insert domain state
    order = Order(customer_id=request.customer_id, amount=request.amount)
    db.add(order)
    db.flush()

    # 2. Stage outbox record
    await app_manager.publisher.publish(
        "orders",
        OutboxPayload.of("orders.v1", {"orderId": order.id, "amount": order.amount}, partition_key=order.customer_id)
    )
    db.commit()
    return {"orderId": order.id, "status": "CONFIRMED"}
```

---

## Configuration Reference

Example `application.yml` for multi-pipeline Spring Boot configuration:

```yaml
outboxify:
  # Global defaults applied to all pipelines
  defaults:
    dialect: POSTGRESQL
    batch-size: 100
    poll-interval-ms: 1000
    processing-timeout-seconds: 60
    max-retries: 5
    immediate-send:
      enabled: true
      timeout-ms: 5000

  # Pipeline-specific definitions
  pipelines:
    orders:
      table-name: ORDERS_OUTBOX
      batch-size: 200

    payments:
      table-name: PAYMENTS_OUTBOX
      poll-interval-ms: 500
      processing-timeout-seconds: 15
      broker:
        # Route through a dedicated named KafkaTemplate bean
        kafka-template-ref: paymentsKafkaTemplate
```

---

## Runnable Examples

Ready-to-run example microservices demonstrating fast-path execution, rollback protection, and database schema setups are available in [`examples/`](file:///Users/timeichinger/coding-stuff/outboxify/examples):

| Example Project | Technology Stack | Description |
| :--- | :--- | :--- |
| **[order-service (Java)](file:///Users/timeichinger/coding-stuff/outboxify/examples/java/order-service)** | Java 21, Spring Boot 4.x, H2 | Dual-write order processing with transactional rollback API |
| **[spring-kafka-example (Java)](file:///Users/timeichinger/coding-stuff/outboxify/examples/java/spring-kafka-example)** | Java 21, Spring Kafka, H2 | Custom `KafkaTemplate` injection & multi-template routing |
| **[order-service (Node.js)](file:///Users/timeichinger/coding-stuff/outboxify/examples/node/order-service)** | Node.js 20+, Express, `@outboxify/core` | Node.js outbox dispatch with REST endpoints |
| **[order-service (Python)](file:///Users/timeichinger/coding-stuff/outboxify/examples/python/order-service)** | Python 3.11+, FastAPI, SQLAlchemy | Async SQLAlchemy session hooks & lifespan integration |

---

## Building & Testing

### Java
```bash
# Run unit tests
mvn clean test

# Run full integration tests with Testcontainers
mvn clean verify
```

### Node.js
```bash
# Run test suite across all packages
npm test --prefix node
```

### Python
```bash
# Run async test suite
pytest python/tests
```

---

## Contributing

We welcome contributions from the community! Check out [CONTRIBUTING.md](CONTRIBUTING.md) for local setup instructions, code style guidelines, and pull request workflows. Maintainers can refer to [RELEASING.md](RELEASING.md) for the release process.

Please also review our [Code of Conduct](CODE_OF_CONDUCT.md) before participating.

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release notes and migration guides.

---

## Security

For vulnerability reporting and our security policy, please refer to [SECURITY.md](SECURITY.md).

---

## License

Outboxify is open-source software licensed under the [Apache License, Version 2.0](LICENSE).
