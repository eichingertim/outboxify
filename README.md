# Outboxify: Universal Polyglot Transactional Outbox Starter

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/Java-21%2B%20%7C%2025%2B-orange.svg)](https://openjdk.org/)
[![Node.js](https://img.shields.io/badge/Node.js-20%2B%20%7C%2022%2B%20%7C%2024%2B-green.svg)](https://nodejs.org/)
[![Python](https://img.shields.io/badge/Python-3.11%2B%20%7C%203.12%2B%20%7C%203.13%2B-blue.svg)](https://www.python.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.x-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Kafka](https://img.shields.io/badge/Kafka-4.x%20KRaft-black.svg)](https://kafka.apache.org/)

**Outboxify** is a modular, high-throughput, polyglot transactional outbox library designed to eliminate dual-write inconsistencies between relational databases and event brokers with guaranteed at-least-once delivery, sub-millisecond fast-path dispatch, and zero vendor lock-in across **Java (Spring Boot 4.x / JDK 21+)**, **Node.js / TypeScript (NestJS 11+ / Prisma 7+ / TypeORM 1+)**, and **Python (FastAPI / SQLAlchemy 2.0+)**.

---

## Polyglot Ecosystem Architecture

```
outboxify/
├── spec/                                   # Language-agnostic specifications & schemas
│   ├── configuration-schema.json           # JSON Schema Draft 2020-12 for application configuration
│   └── state-machine.md                    # State machine lifecycle rules
├── java/                                   # Java Implementation (JDK 21+ / Spring Boot 4.x / Kafka 4.x)
│   ├── outboxify-core                     # Core runtime, models, state engine & SPIs
│   ├── outboxify-dialects                 # Oracle, PostgreSQL, MySQL, MSSQL dialects & dynamic SQL
│   ├── outboxify-kafka                    # Resilient Kafka 4.x producer wrapper with async futures
│   └── outboxify-spring-boot-starter      # Spring Boot 4.x AutoConfiguration, JPA listeners, hooks
├── node/                                   # Node.js / TypeScript Implementation (Node 20+ / 22+ / 24+)
│   ├── outboxify-core                     # Core dialect registry, state engine, poller/reaper & TS types
│   ├── outboxify-typeorm                  # TypeORM entity subscriber & commit hook
│   ├── outboxify-prisma                   # Prisma client extension ($extends / query hook)
│   └── outboxify-nestjs                   # NestJS 11+ dynamic module & lifecycle manager
└── python/                                 # Python Implementation (Python 3.11+ / 3.12+ / 3.13+ / PEP 561)
    ├── outboxify/                         # Pure Python AsyncIO runtime & dialects
    │   ├── dialects.py                     # Oracle, Postgres, MySQL, MSSQL, SQLite dialects
    │   ├── sqlalchemy.py                   # SQLAlchemy 2.0+ after_commit session event manager & mixin
    │   └── fastapi.py                      # FastAPI lifespan context manager & DI provider
    └── tests/                              # Pytest async test suite
```

---

## Dialect Concurrency Matrix

| Capability | Oracle (12c+) | PostgreSQL (9.5+) | MySQL (8.0+) | MS SQL Server (2019+) |
| :--- | :--- | :--- | :--- | :--- |
| **Row Locking** | `FOR UPDATE SKIP LOCKED` | `FOR UPDATE SKIP LOCKED` | `FOR UPDATE SKIP LOCKED` | `WITH (UPDLOCK, READPAST, ROWLOCK)` |
| **Paging** | `FETCH FIRST :n ROWS ONLY` | `LIMIT :n` | `LIMIT :n` | `TOP (:n)` |
| **Timestamp** | `SYSTIMESTAMP` | `CURRENT_TIMESTAMP` | `NOW(6)` | `SYSUTCDATETIME()` |
| **Interval** | `NUMTODSINTERVAL(:s, 'SECOND')` | `INTERVAL ':s SECONDS'` | `INTERVAL :s SECOND` | `DATEADD(second, -:s, SYSUTCDATETIME())` |
| **Sparse Indexing** | Function-Based Index | Partial Index (`WHERE`) | Composite Index | Filtered Index (`WHERE`) |

---

## Polyglot Usage Guides

### 1. Java (Spring Boot 4.x / JDK 21+)

```java
@Service
public class OrderService {
    private final OutboxPublisher outboxPublisher;

    public OrderService(OutboxPublisher outboxPublisher) {
        this.outboxPublisher = outboxPublisher;
    }

    @Transactional
    public void createOrder(OrderDto dto) {
        outboxPublisher.publish("orders", OutboxPayload.builder()
                .topic("orders.v1")
                .partitionKey(dto.getCustomerId())
                .payload("{\"orderId\":\"" + dto.getOrderId() + "\"}")
                .header("eventType", "OrderCreated")
                .build());
    }
}
```

> **Spring Kafka Integration**: Outboxify automatically detects and binds to your application's `KafkaTemplate` (or `spring.kafka.*` configuration) without requiring duplicate broker definitions. Custom or named `KafkaTemplate` beans can also be assigned per-pipeline using `outboxify.pipelines.<pipeline>.broker.kafka-template-ref`.

### 2. Node.js (TypeORM, Prisma & NestJS)

```typescript
// TypeORM Subscriber
const subscriber = new OutboxEntitySubscriber(hook);

// Prisma Client Extension
const prisma = new PrismaClient().$extends(createOutboxifyPrismaExtension(hook, ['Order']));

// NestJS Dynamic Module
@Module({
  imports: [
    OutboxifyModule.forRoot({
      pipelines: {
        orders: new PipelineConfig({ name: 'orders', tableName: 'ORDERS' }),
      },
      repository: new SqlOutboxRepository(dbPool),
      brokerPublisher: new KafkaBrokerPublisher(kafkaProducer)
    })
  ]
})
export class AppModule {}
```

### 3. Python (SQLAlchemy & FastAPI)

```python
# SQLAlchemy Mixin & Session Hook
class Order(Base, OutboxRecordMixin):
    __tablename__ = "orders"
    id = Column(String, primary_key=True)
    outbox_topic = Column(String, default="orders.v1")

# FastAPI Lifespan Registration
app_manager = OutboxifyAppManager(pipelines, repo, broker)
app = FastAPI(lifespan=create_outboxify_lifespan(app_manager))

@app.post("/orders")
async def create_order(dto: OrderDto):
    await app_manager.publisher.publish("orders", OutboxPayload.of("orders.v1", dto.dict(), partition_key=dto.customer_id))
    return {"status": "created"}
```

---

## Microservice Examples

Ready-to-run microservice examples showcasing fast-path dual-write consistency, transaction rollback safety, and background poller recovery are available in the [`examples/`](file:///Users/timeichinger/coding-stuff/outboxify/examples) directory:

- **[Java Spring Boot Order Service](file:///Users/timeichinger/coding-stuff/outboxify/examples/java/order-service)** (`examples/java/order-service`): Spring Boot 4.x + in-memory H2 + Outboxify starter. Run with `mvn spring-boot:run`.
- **[Java Spring KafkaTemplate Example](file:///Users/timeichinger/coding-stuff/outboxify/examples/java/spring-kafka-example)** (`examples/java/spring-kafka-example`): Spring Boot 4.x + Spring Kafka + custom injected `KafkaTemplate` routing. Run with `mvn spring-boot:run`.
- **[Node.js Order Service](file:///Users/timeichinger/coding-stuff/outboxify/examples/node/order-service)** (`examples/node/order-service`): Express + `@outboxify/core`. Run with `npm start`.
- **[Python FastAPI Order Service](file:///Users/timeichinger/coding-stuff/outboxify/examples/python/order-service)** (`examples/python/order-service`): FastAPI + SQLAlchemy + `outboxify`. Run with `uvicorn app.main:app`.

---

## Verification & Testing

Execute test suites across all three runtimes:

```bash
# Java (All submodules & Testcontainers)
mvn clean verify

# Node.js (Core, TypeORM, Prisma, NestJS)
npm test --prefix node

# Python (AsyncIO, Dialects, SQLAlchemy, FastAPI)
pytest python/tests
```

---

## License

Licensed under the Apache License, Version 2.0.
