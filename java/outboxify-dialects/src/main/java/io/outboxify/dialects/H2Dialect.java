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

package io.outboxify.dialects;

import io.outboxify.core.model.ColumnMapping;
import io.outboxify.core.spi.DatabaseDialect;
import io.outboxify.core.spi.DialectType;

/**
 * H2 in-memory database dialect for high-speed local testing and verification.
 */
public class H2Dialect implements DatabaseDialect {

    @Override
    public DialectType getDialectType() {
        return DialectType.H2;
    }

    @Override
    public String getLockingClause() {
        return "FOR UPDATE";
    }

    @Override
    public String getTimestampFunction() {
        return "CURRENT_TIMESTAMP";
    }

    @Override
    public String getStaleThresholdExpression(int seconds) {
        return "DATEADD('SECOND', -" + seconds + ", CURRENT_TIMESTAMP)";
    }

    @Override
    public String buildSelectBatchForUpdateQuery(String tableName, ColumnMapping columns, int batchSize, int maxRetries) {
        return "SELECT " +
                columns.getId() + ", " +
                columns.getTopic() + ", " +
                columns.getPartitionKey() + ", " +
                columns.getPayload() + ", " +
                columns.getHeaders() + ", " +
                columns.getStatus() + ", " +
                columns.getRetryCount() + ", " +
                columns.getLastError() + ", " +
                columns.getCreatedAt() + ", " +
                columns.getUpdatedAt() + ", " +
                columns.getProcessedAt() +
                " FROM " + tableName +
                " WHERE " + columns.getStatus() + " IN ('NEW', 'FAILED')" +
                " AND " + columns.getRetryCount() + " < " + maxRetries +
                " ORDER BY " + columns.getCreatedAt() + " ASC" +
                " LIMIT " + batchSize +
                " FOR UPDATE";
    }

    @Override
    public String buildReapStaleQuery(String tableName, ColumnMapping columns, int timeoutSeconds, int maxRetries) {
        return "UPDATE " + tableName +
                " SET " + columns.getStatus() + " = 'FAILED', " +
                columns.getRetryCount() + " = " + columns.getRetryCount() + " + 1, " +
                columns.getLastError() + " = 'PROCESSING_TIMEOUT_EXCEEDED', " +
                columns.getUpdatedAt() + " = CURRENT_TIMESTAMP" +
                " WHERE " + columns.getStatus() + " = 'PROCESSING'" +
                " AND " + columns.getUpdatedAt() + " < DATEADD('SECOND', -" + timeoutSeconds + ", CURRENT_TIMESTAMP)";
    }

    @Override
    public String getSparseIndexDdl(String tableName, String indexName, ColumnMapping columns) {
        return "CREATE INDEX " + indexName + " ON " + tableName + " (" +
                columns.getStatus() + ", " + columns.getCreatedAt() + ")";
    }

    @Override
    public boolean supportsSkipLocked() {
        return false;
    }
}
