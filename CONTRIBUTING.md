# Contributing to Outboxify

Thank you for your interest in contributing to **Outboxify**! Outboxify is a polyglot transactional outbox library with core implementations across **Java**, **Node.js / TypeScript**, and **Python**.

We welcome contributions of all kinds: bug fixes, new database dialects, framework integrations, performance improvements, and documentation.

---

## 1. Development Prerequisites

Ensure you have the required toolchains installed for the runtime you wish to contribute to:

- **Java Runtime**: JDK 21+ (OpenJDK or Eclipse Temurin) and Apache Maven 3.9+
- **Node.js Runtime**: Node.js 20.0.0+ (Node 22 / 24 recommended) and npm 10+
- **Python Runtime**: Python 3.11+ (3.12 / 3.13 supported) and `pip`
- **Docker**: Required for running the integration test suites with Testcontainers

---

## 2. Monorepo Structure

```
outboxify/
├── spec/                             # Language-agnostic configuration schemas and state machine specs
├── java/                             # Java Implementation (Spring Boot 4.x / JDK 21+)
│   ├── outboxify-core/               # Core state machine, models, SPIs
│   ├── outboxify-dialects/           # Oracle, Postgres, MySQL, MSSQL dialect implementations
│   ├── outboxify-kafka/              # Resilient Kafka broker publisher
│   └── outboxify-spring-boot-starter/# Spring Boot auto-configuration & JPA hooks
├── node/                             # Node.js / TypeScript Implementation (Node 20+)
│   ├── outboxify-core/               # Core runtime, dialect registry, poller, reaper & TS types
│   ├── outboxify-typeorm/            # TypeORM subscriber integration
│   ├── outboxify-prisma/             # Prisma client extension
│   └── outboxify-nestjs/             # NestJS 11+ dynamic module
├── python/                           # Python Implementation (Python 3.11+ / PEP 561)
│   ├── outboxify/                    # Core asyncio runtime, dialects, SQLAlchemy & FastAPI
│   └── tests/                        # Pytest async test suite
└── .github/                          # Workflows, issue templates, dependabot
```

---

## 3. Building and Running Tests

Before submitting a Pull Request, verify tests across all modified runtimes:

### Java
```bash
mvn clean verify
```

### Node.js
```bash
cd node
npm test
```

### Python
```bash
cd python
pytest
```

---

## 4. Coding & Architecture Standards

### Core Invariant Rules
1. **Zero Dual-Write Race Conditions**: Fast-path publishing must execute *strictly after* database transaction commit (e.g., Spring `TransactionSynchronization.afterCommit`, TypeORM `commitTransaction` hook, SQLAlchemy `after_commit`).
2. **Locking & Concurrency**: Never perform full table scans or unindexed table locks. Always use non-blocking row-level locks (`FOR UPDATE SKIP LOCKED` or `WITH (UPDLOCK, READPAST, ROWLOCK)`).
3. **Reaper Watchdog Safety**: The background watchdog must only reap stale `PROCESSING` rows older than `processingTimeoutSeconds`, incrementing `retryCount` up to `maxRetries`.
4. **Zero-CVE Baseline**: Keep all dependencies up-to-date with zero critical or high vulnerabilities.

### Conventional Commits
We use [Conventional Commits](https://www.conventionalcommits.org/) for automated changelogs:

- `feat(java)`: Add new dialect or feature
- `fix(node)`: Fix poller reconnection issue
- `perf(python)`: Optimize async query batching
- `docs`: Documentation improvements
- `refactor`: Code refactoring without behavioral change
- `test`: Adding or improving tests
- `chore`: Dependency updates, CI/CD changes

---

## 5. Pull Request Process

1. **Fork and Branch**: Create a feature branch from `main` (e.g., `feat/dynamodb-dialect` or `fix/reaper-interval`).
2. **Implement and Test**: Add unit and integration tests covering positive and negative cases.
3. **Commit**: Follow Conventional Commits formatting.
4. **Submit PR**: Complete the [Pull Request Template](.github/PULL_REQUEST_TEMPLATE.md) with details on tested databases/brokers.
5. **Review**: Ensure CI passes all quality gates (Java, Node, Python, CodeQL).

---

## 6. License

By contributing to Outboxify, you agree that your contributions will be licensed under the [Apache License, Version 2.0](LICENSE).
