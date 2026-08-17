# Open-Source Specification & Architecture: Outboxify (Universal Transactional Outbox)
**Document Version:** 0.1.0  
**License:** Apache-2.0  
**Project Ecosystem:** Polyglot Transactional Outbox Engine (Java, TypeScript, Python)  
**Supported Databases:** Oracle (12c+), PostgreSQL (9.5+), MySQL (8.0+), Microsoft SQL Server (2019+)  
**Supported Message Brokers:** Apache Kafka (Primary), Redpanda, RabbitMQ (Pluggable)  

---

## 1. Project Vision & Architecture Principles

**Outboxify** is a modular, high-throughput, polyglot transactional outbox library designed to solve dual-write consistency between relational databases and event brokers with guaranteed at-least-once delivery, sub-millisecond dispatch capability, and zero vendor lock-in.

### Core Design Principles
* **Database Agnostic via Dialects:** Encapsulate database-specific concurrency control (`FOR UPDATE SKIP LOCKED`, pagination, timestamps) behind a unified `DatabaseDialect` SPI.
* **Polyglot Design Specification:** A unified state machine, configuration schema, and behavioral specification implemented across language-specific modules (`outboxify-java`, `outboxify-node`, `outboxify-python`).
* **Zero Phantom Reads / Dual-Write Safety:** Fast-path sends occur strictly *after* transaction commits. Slow-path poller handles network partitions and crashes.
* **Flexible Storage Topologies:** Full support for both **Dedicated Outbox Tables** and **In-Entity / Embedded Domain Tables**.
* **Zero Mandatory Dependencies in Core:** Core engine relies on native SQL drivers/connections; framework integrations (Spring Boot, NestJS, FastAPI) are isolated in dedicated extension modules.

---

## 2. Multi-Module Repository Structure

```
outboxify/
├── spec/                                   # Language-agnostic specifications & test vectors
│   ├── configuration-schema.json           # JSON Schema for application configs
│   └── state-machine.md                    # Canonical lifecycle rules
├── java/                                   # Java Implementation (JDK 21+ / Spring Boot 4.x)
│   ├── outboxify-core                     # Pure Java runtime, SQL dialect registry, state engine
│   ├── outboxify-dialects                 # Oracle, Postgres, MySQL, MSSQL dialect implementations
│   ├── outboxify-kafka                    # Kafka 4.x producer wrapper & batch dispatcher
│   └── outboxify-spring-boot-starter      # Spring Boot 4.x AutoConfiguration, hooks, JPA listeners
├── node/                                   # TypeScript / Node.js Implementation (v20+)
│   ├── outboxify-core                     # Core scheduling, state machine, DB abstraction
│   ├── outboxify-prisma                   # Prisma extension / middleware
│   ├── outboxify-typeorm                  # TypeORM subscriber & decorator
│   └── outboxify-nestjs                   # NestJS dynamic module
├── python/                                 # Python Implementation (3.11+)
│   ├── outboxify-core                     # AsyncIO poller, dynamic query builder
│   ├── outboxify-sqlalchemy               # SQLAlchemy event hooks & session integration
│   └── outboxify-fastapi                  # FastAPI middleware and lifespan manager
└── e2e-testsuite/                          # Polyglot matrix tests using Testcontainers
    ├── docker-compose.yml
    └── scenarios/                          # Multi-DB, multi-pod failure tests
```

---

## 3. Database Dialect Abstraction Layer (SPI)

Every database implements the `DatabaseDialect` contract to supply optimized SQL templates for locking, paging, and timeout calculations.

### Dialect Matrix

| Capability | Oracle (12c+) | PostgreSQL (9.5+) | MySQL (8.0+) | MS SQL Server (2019+) |
| :--- | :--- | :--- | :--- | :--- |
| **Row Locking** | `FOR UPDATE SKIP LOCKED` | `FOR UPDATE SKIP LOCKED` | `FOR UPDATE SKIP LOCKED` | `WITH (UPDLOCK, READPAST, ROWLOCK)` |
| **Limit / Paging** | `FETCH FIRST :n ROWS ONLY` | `LIMIT :n` | `LIMIT :n` | `TOP (:n)` |
| **Timestamp Function** | `SYSTIMESTAMP` | `CURRENT_TIMESTAMP` | `NOW(6)` | `SYSUTCDATETIME()` |
| **Interval Expression** | `NUMTODSINTERVAL(:s, 'SECOND')` | `INTERVAL ':s SECONDS'` | `INTERVAL :s SECOND` | `DATEADD(second, -:s, SYSUTCDATETIME())` |
| **Sparse Indexing** | Function-Based Index | Partial Index (`WHERE`) | Functional Index | Filtered Index (`WHERE`) |

---

## 4. State Machine & Execution Flow

```
                      +-----------------------------+
                      |      DB Record Created      |
                      |        Status: NEW          |
                      +--------------+--------------+
                                     |
              +----------------------+----------------------+
              |                                             |
   [Fast-Path: Commit Hook]                       [Slow-Path: Background Poller]
              |                                             |
              v                                             v
    (afterCommit Trigger)                        (Fetch SKIP LOCKED Batch)
              |                                             |
              v                                             v
     Send to Kafka Async                         Mark Batch: PROCESSING
              |                                             |
     +--------+--------+                                    v
     |                 |                          Send Batch to Kafka
 (Kafka ACK)      (Kafka Fail)                              |
     |                 |                            +-------+-------+
     v                 v                            |               |
Mark: SENT        Mark: FAILED                 (Kafka ACK)     (Kafka Fail / Crash)
                       |                            |               |
                       |                            v               v
                       +------------------->   Mark: SENT     [Reaper Watchdog]
                                                              Resets to FAILED/NEW
```

---

## 5. Polyglot Architecture & API Contracts

### 5.1 Java Module (`outboxify-spring-boot-starter`)

```java
public interface OutboxPublisher {
    String stage(String pipeline, OutboxPayload message);
    CompletableFuture<OutboxResult> publish(String pipeline, OutboxPayload message);
}

public interface OutboxHook {
    void registerForCommit(String pipeline, String recordId, OutboxPayload message);
}

public interface OutboxRecord {
    String getOutboxPipeline();
    String getOutboxId();
    String getTopic();
    String getPartitionKey();
    String getPayload();
    Map<String, String> getHeaders();
}
```

### 5.2 TypeScript / Node.js Module (`outboxify-node`)

```typescript
import { OutboxRecord, OutboxEntitySubscriber } from '@outboxify/typeorm';

@Entity('ORDERS')
@EventSubscriber()
export class OrderEntity implements OutboxRecord {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ name: 'OUTBOX_STATUS', default: 'NEW' })
  outboxStatus: string;

  @Column({ name: 'KAFKA_TOPIC' })
  kafkaTopic: string;

  @Column({ name: 'PAYLOAD', type: 'text' })
  outboxPayload: string;

  getOutboxId(): string { return this.id; }
  getTopic(): string { return this.kafkaTopic; }
  getPayload(): string { return this.outboxPayload; }
}
```

### 5.3 Python Module (`outboxify-py`)

```python
from outboxify import register_after_commit_hook, OutboxRecordMixin
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Order(DeclarativeBase, OutboxRecordMixin):
    __tablename__ = "orders"
    
    id: Mapped[str] = mapped_column(primary_key=True)
    outbox_status: Mapped[str] = mapped_column(default="NEW")
    kafka_topic: Mapped[str] = mapped_column(default="orders.v1")
    outbox_payload: Mapped[str] = mapped_column()
```

---

## 6. Unified Configuration Specification (`outboxify.yml`)

```yaml
outboxify:
  defaults:
    batch-size: 100
    poll-interval-ms: 1000
    processing-timeout-seconds: 300
    max-retries: 5

  pipelines:
    orders:
      enabled: true
      table-name: "ORDERS"
      dialect: "ORACLE" # ORACLE | POSTGRESQL | MYSQL | MSSQL | AUTO_DETECT
      batch-size: 200
      poll-interval-ms: 500
      processing-timeout-seconds: 180
      immediate-send:
        enabled: true
        mode: "TRANSACTION_HOOK"
      columns:
        id: "ORDER_ID"
        topic: "KAFKA_TOPIC"
        partition-key: "CUSTOMER_ID"
        payload: "OUTBOX_PAYLOAD"
        headers: "OUTBOX_HEADERS"
        status: "OUTBOX_STATUS"
        retry-count: "RETRY_COUNT"
        last-error: "LAST_ERROR_MSG"
        created-at: "CREATED_AT"
        updated-at: "UPDATED_AT"
        processed-at: "PROCESSED_AT"
      broker:
        type: "KAFKA"
        producer:
          bootstrap-servers: "localhost:9092"
          acks: "all"
          enable-idempotence: true
          linger-ms: 5
```

---

## 7. Open-Source Ecosystem & Contribution Standards

### 7.1 Testing Harness (Testcontainers Matrix)
* **Databases:** Oracle Free 23c, PostgreSQL 16, MySQL 8.4 LTS, Azure SQL Edge (MSSQL).
* **Brokers:** Apache Kafka 3.7+ (KRaft), Redpanda.
* **Test Scenarios:**
  1. *Concurrent Contention:* 10 worker pods, 50k messages, 0 duplicate processing, 0 deadlocks.
  2. *Split-Brain & Crash Recovery:* Worker pod killed during `PROCESSING` phase; Reaper recovers rows within $T_{	ext{timeout}}$.
  3. *Rollback Safety:* Domain transaction rollback produces 0 messages published to Kafka.

### 7.2 Release & Governance
* **Versioning:** Semantic Versioning (`vMAJOR.MINOR.PATCH`).
* **License:** Apache License 2.0.
* **Security:** Automated CodeQL and Dependabot scanning across all polyglot submodules.
