---
name: outbox-dialect-implementer
description: Skill for implementing and testing new SQL database dialects across Java, Node.js, and Python in Outboxify.
---

# Outbox Dialect Implementer Skill

Use this skill when implementing support for a new relational or distributed SQL database dialect (e.g. CockroachDB, Google Cloud Spanner, TiDB, DB2, SQLite).

## Step-by-Step Implementation Workflow

1. **Review Dialect Concurrency Requirements**:
   - Determine non-blocking row-level lock syntax (`FOR UPDATE SKIP LOCKED` or engine-equivalent).
   - Determine pagination syntax (`LIMIT :n`, `FETCH FIRST :n ROWS ONLY`, `TOP (:n)`).
   - Determine UTC timestamp functions (`CURRENT_TIMESTAMP`, `SYSTIMESTAMP`, `NOW(6)`, `SYSUTCDATETIME()`).
   - Determine interval arithmetic (`INTERVAL :s SECOND`, `DATEADD(second, -:s, ...)`).
   - Determine sparse or partial index syntax (`WHERE status IN ('NEW', 'FAILED')`).

2. **Implement in Java (`outboxify-dialects`)**:
   - Create `NewDbDialect.java` in `java/outboxify-dialects/src/main/java/io/outboxify/dialects/` extending `DatabaseDialect`.
   - Register in `DialectType` enum and `DialectRegistry.java`.
   - Add unit tests in `DialectsTest.java`.

3. **Implement in Node.js (`outboxify-core`)**:
   - Create `newdb-dialect.js` in `node/outboxify-core/src/dialects/` extending `DatabaseDialect`.
   - Register in `DialectRegistry` and export in `index.js` and `index.d.ts`.
   - Add unit test in `core.test.js`.

4. **Implement in Python (`outboxify`)**:
   - Implement dialect class in `python/outboxify/dialects.py`.
   - Add unit test in `python/tests/test_outboxify.py`.

5. **Update Specification**:
   - Add dialect enum value to `spec/configuration-schema.json`.
   - Document concurrency characteristics in `spec/state-machine.md` and `README.md`.
