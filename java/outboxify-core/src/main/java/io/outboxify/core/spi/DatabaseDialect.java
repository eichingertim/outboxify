/*
 * Copyright 2026 Outboxify Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.outboxify.core.spi;

import io.outboxify.core.model.ColumnMapping;

/**
 * Service Provider Interface (SPI) for database-specific SQL dialect implementations.
 * Encapsulates concurrency control (`FOR UPDATE SKIP LOCKED` / `READPAST`),
 * pagination syntax, timestamp arithmetic, and sparse indexing strategies.
 */
public interface DatabaseDialect {

    /**
     * Identifies the dialect type.
     *
     * @return the dialect enum
     */
    DialectType getDialectType();

    /**
     * Row locking clause for concurrency control (e.g., `FOR UPDATE SKIP LOCKED`).
     *
     * @return the SQL lock clause
     */
    String getLockingClause();

    /**
     * Current system timestamp function in the database (e.g., `SYSTIMESTAMP`, `CURRENT_TIMESTAMP`, `NOW(6)`).
     *
     * @return timestamp function SQL expression
     */
    String getTimestampFunction();

    /**
     * Builds a SQL timestamp expression representing `(CURRENT_TIME - N seconds)`.
     *
     * @param seconds the timeout in seconds
     * @return SQL expression
     */
    String getStaleThresholdExpression(int seconds);

    /**
     * Builds a SELECT batch query with row-level non-blocking locking.
     *
     * @param tableName table name
     * @param columns column mappings
     * @param batchSize max rows to fetch
     * @param maxRetries retry ceiling
     * @return executable SQL query with parameter placeholders where appropriate
     */
    String buildSelectBatchForUpdateQuery(String tableName, ColumnMapping columns, int batchSize, int maxRetries);

    /**
     * Builds a batch update query to reset stale `PROCESSING` rows back to `FAILED` or `NEW`.
     *
     * @param tableName table name
     * @param columns column mappings
     * @param timeoutSeconds age threshold in seconds
     * @param maxRetries retry ceiling
     * @return executable SQL query
     */
    String buildReapStaleQuery(String tableName, ColumnMapping columns, int timeoutSeconds, int maxRetries);

    /**
     * Generates a sparse / partial / function-based index DDL definition for the table's status column.
     *
     * @param tableName table name
     * @param indexName index name
     * @param columns column mappings
     * @return DDL statement
     */
    String getSparseIndexDdl(String tableName, String indexName, ColumnMapping columns);

    /**
     * Indicates whether this dialect supports non-blocking `SKIP LOCKED` or equivalent row locking.
     *
     * @return true if non-blocking row locks are supported
     */
    default boolean supportsSkipLocked() {
        return true;
    }
}
