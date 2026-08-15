# AGENTS.md — Outboxify Developer & AI Agent Guide

Welcome, AI Agent or human contributor. This document defines operational standards, invariant rules, and key commands for the **Outboxify** polyglot repository.

---

## 1. Project Overview & Architecture

**Outboxify** is a polyglot transactional outbox library ensuring dual-write consistency between relational databases and Apache Kafka / event brokers with guaranteed at-least-once delivery.

```
outboxify/
├── spec/                                   # Canonical schemas & state machine specification
├── java/                                   # Spring Boot 4.x / JDK 21+ (Core, Dialects, Kafka, Starter)
├── node/                                   # Node 20+ / TS (Core, TypeORM, Prisma, NestJS 11+)
├── python/                                 # Python 3.11+ / PEP 561 (Core, Dialects, SQLAlchemy, FastAPI)
└── .github/                                # CI/CD workflows, CodeQL, Dependabot
```

---

## 2. Invariant Rules & Architectural Guardrails

Agents modifying this codebase MUST uphold these rules:

1. **Strict Fast-Path Post-Commit Safety**: Fast-path publishing MUST only occur *after* the database transaction commits (e.g., Spring `afterCommit`, TypeORM `commitTransaction` hook, SQLAlchemy `after_commit`). NEVER publish within an uncommitted transaction.
2. **Non-Blocking Row Concurrency**: All database polling queries MUST use non-blocking locking (`FOR UPDATE SKIP LOCKED` or `WITH (UPDLOCK, READPAST, ROWLOCK)`). NEVER perform table locks or unindexed queries.
3. **Reaper Watchdog Contract**: Orphaned `PROCESSING` rows exceeding `processingTimeoutSeconds` MUST be transitioned to `FAILED` with incremented `retryCount`, retrying up to `maxRetries`.
4. **Zero Vendor Lock-in Core**: Core modules (`outboxify-core`) must remain framework-free and rely only on standard SPI interfaces (`DatabaseDialect`, `BrokerPublisher`, `OutboxRepository`). Framework starter modules (`outboxify-spring-boot-starter`, `outboxify-nestjs`, `outboxify-fastapi`) handle IoC/DI.
5. **Zero-CVE Dependency Standard**: Maintain latest production patched dependencies across Maven, npm, and PyPI.

---

## 3. Key CLI Commands

### Java (JDK 21+, Maven 3.9+)
```bash
# Run all unit tests
mvn clean test

# Run full build with integration tests (requires Docker for Testcontainers)
mvn clean verify
```

### Node.js (Node 20+, npm 10+)
```bash
# Run unit tests across all workspaces
npm test --prefix node

# Run tests in specific package
npm test --prefix node/outboxify-core
```

### Python (Python 3.11+, pytest)
```bash
# Install editable with all extras
cd python && pip install -e ".[test,sqlalchemy,fastapi,kafka]"

# Run async test suite
pytest python/tests
```

---

## 4. Code Style & Typing Standards

- **Java**: Java 21 LTS standard, strict records/immutability where appropriate, full Javadoc on SPI contracts, SLF4J 2.x for logging.
- **Node.js**: Clean CommonJS modules with strict `.d.ts` TypeScript declaration files matching exports.
- **Python**: Python 3.11+ async/await, type annotations on all functions (`PEP 484`), `py.typed` marker file maintained (`PEP 561`).
- **Commits**: Follow [Conventional Commits](https://www.conventionalcommits.org/) (e.g., `feat(dialects): add CockroachDB support`, `fix(reaper): prevent duplicate retry increment`).

---

## 5. Adding a Database Dialect

When adding a new database dialect:
1. Implement `DatabaseDialect` in `java/outboxify-dialects/src/main/java/io/outboxify/dialects/`
2. Implement dialect in `node/outboxify-core/src/dialects/` and update registry
3. Implement dialect in `python/outboxify/dialects.py`
4. Register the new dialect in `spec/configuration-schema.json` and add corresponding unit test vectors in `tests`.
