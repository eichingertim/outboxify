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
 * Microsoft SQL Server 2019+ dialect supporting table hints WITH (UPDLOCK, READPAST, ROWLOCK),
 * TOP paging, SYSUTCDATETIME(), and Filtered Indexes.
 */
public class SqlServerDialect implements DatabaseDialect {

    @Override
    public DialectType getDialectType() {
        return DialectType.MSSQL;
    }

    @Override
    public String getLockingClause() {
        return "WITH (UPDLOCK, READPAST, ROWLOCK)";
    }

    @Override
    public String getTimestampFunction() {
        return "SYSUTCDATETIME()";
    }

    @Override
    public String getStaleThresholdExpression(int seconds) {
        return "DATEADD(second, -" + seconds + ", SYSUTCDATETIME())";
    }

    @Override
    public String buildSelectBatchForUpdateQuery(String tableName, ColumnMapping columns, int batchSize, int maxRetries) {
        return "SELECT TOP (" + batchSize + ") " +
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
                " FROM " + tableName + " WITH (UPDLOCK, READPAST, ROWLOCK)" +
                " WHERE " + columns.getStatus() + " IN ('NEW', 'FAILED')" +
                " AND " + columns.getRetryCount() + " < " + maxRetries +
                " ORDER BY " + columns.getCreatedAt() + " ASC";
    }

    @Override
    public String buildReapStaleQuery(String tableName, ColumnMapping columns, int timeoutSeconds, int maxRetries) {
        return "UPDATE " + tableName +
                " SET " + columns.getStatus() + " = 'FAILED', " +
                columns.getRetryCount() + " = " + columns.getRetryCount() + " + 1, " +
                columns.getLastError() + " = 'PROCESSING_TIMEOUT_EXCEEDED', " +
                columns.getUpdatedAt() + " = SYSUTCDATETIME()" +
                " WHERE " + columns.getStatus() + " = 'PROCESSING'" +
                " AND " + columns.getUpdatedAt() + " < DATEADD(second, -" + timeoutSeconds + ", SYSUTCDATETIME())";
    }

    @Override
    public String getSparseIndexDdl(String tableName, String indexName, ColumnMapping columns) {
        return "CREATE NONCLUSTERED INDEX " + indexName + " ON " + tableName + " (" +
                columns.getCreatedAt() + ") WHERE " +
                columns.getStatus() + " IN ('NEW', 'FAILED')";
    }

    @Override
    public boolean supportsSkipLocked() {
        return true;
    }
}
