# Changelog

All notable changes to the **Outboxify** project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] - 2026-08-17

### Added
- **Polyglot Core Architecture**: Language-agnostic specification and unified state machine for transactional outbox pattern.
- **Java Runtime (`io.outboxify`)**:
  - `outboxify-core`: Pure Java SPI interfaces (`BrokerPublisher`, `OutboxRepository`, `DatabaseDialect`), in-memory scheduler, and transactional publisher.
  - `outboxify-dialects`: High-performance dialects with non-blocking row locking for PostgreSQL, Oracle, MySQL, and MS SQL Server.
  - `outboxify-kafka`: Resilient Apache Kafka 4.x producer wrapper with asynchronous futures.
  - `outboxify-spring-boot-starter`: Spring Boot 4.x autoconfiguration, post-commit transaction synchronization hooks, lifecycle manager, and native `KafkaTemplate` auto-detection / per-pipeline routing.
- **Node.js / TypeScript Runtime (`@outboxify/*`)**:
  - `@outboxify/core`: TypeScript runtime, dialect registry, poller engine, reaper watchdog, and TypeScript declarations.
  - `@outboxify/typeorm`: TypeORM subscriber with transaction commit hooks.
  - `@outboxify/prisma`: Prisma client extension (`$extends`) for outbox event staging.
  - `@outboxify/nestjs`: NestJS 11+ dynamic module and lifecycle service.
- **Python Runtime (`outboxify`)**:
  - Pure Python AsyncIO runtime, SQL dialects, and poller/reaper tasks.
  - SQLAlchemy 2.0+ `after_commit` session event listener and outbox mixin.
  - FastAPI lifespan manager and dependency injection provider.
- **Runnable Example Microservices**:
  - Java: `order-service` (Spring Boot 4.x + H2)
  - Java: `spring-kafka-example` (Spring Boot 4.x + custom `KafkaTemplate` routing)
  - Node.js: `order-service` (Express + `@outboxify/core`)
  - Python: `order-service` (FastAPI + SQLAlchemy)
- **CI/CD & Security**:
  - GitHub Actions matrix workflow testing Java 21/25, Node 20/22/24, Python 3.11/3.12/3.13, and Testcontainers.
  - Automated release workflow building Java JARs, npm tarballs, and Python wheels.
  - Branch protection on `main` enforcing CI checks, review approvals, and conversation resolution.
