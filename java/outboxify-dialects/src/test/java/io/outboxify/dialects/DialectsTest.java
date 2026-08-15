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
import io.outboxify.core.spi.DialectType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DialectsTest {

    private final ColumnMapping columns = ColumnMapping.builder()
            .id("ORDER_ID")
            .topic("KAFKA_TOPIC")
            .partitionKey("CUSTOMER_ID")
            .payload("OUTBOX_PAYLOAD")
            .headers("OUTBOX_HEADERS")
            .status("OUTBOX_STATUS")
            .retryCount("RETRY_COUNT")
            .lastError("LAST_ERROR_MSG")
            .createdAt("CREATED_AT")
            .updatedAt("UPDATED_AT")
            .processedAt("PROCESSED_AT")
            .build();

    @Test
    void testOracleDialectSql() {
        OracleDialect dialect = new OracleDialect();
        assertThat(dialect.getDialectType()).isEqualTo(DialectType.ORACLE);
        assertThat(dialect.getLockingClause()).isEqualTo("FOR UPDATE SKIP LOCKED");
        assertThat(dialect.getTimestampFunction()).isEqualTo("SYSTIMESTAMP");
        assertThat(dialect.getStaleThresholdExpression(300)).contains("NUMTODSINTERVAL(300, 'SECOND')");

        String query = dialect.buildSelectBatchForUpdateQuery("ORDERS", columns, 200, 5);
        assertThat(query).contains("FETCH FIRST 200 ROWS ONLY FOR UPDATE SKIP LOCKED")
                .contains("ORDER BY CREATED_AT ASC")
                .contains("WHERE OUTBOX_STATUS IN ('NEW', 'FAILED')")
                .contains("AND RETRY_COUNT < 5");

        String reap = dialect.buildReapStaleQuery("ORDERS", columns, 300, 5);
        assertThat(reap).contains("UPDATE ORDERS")
                .contains("NUMTODSINTERVAL(300, 'SECOND')");

        String indexDdl = dialect.getSparseIndexDdl("ORDERS", "IDX_OUTBOX_SPARSE", columns);
        assertThat(indexDdl).contains("CASE WHEN OUTBOX_STATUS IN ('NEW', 'FAILED') THEN OUTBOX_STATUS ELSE NULL END");
    }

    @Test
    void testPostgresDialectSql() {
        PostgresDialect dialect = new PostgresDialect();
        assertThat(dialect.getDialectType()).isEqualTo(DialectType.POSTGRESQL);
        assertThat(dialect.getLockingClause()).isEqualTo("FOR UPDATE SKIP LOCKED");
        assertThat(dialect.getTimestampFunction()).isEqualTo("CURRENT_TIMESTAMP");

        String query = dialect.buildSelectBatchForUpdateQuery("ORDERS", columns, 100, 3);
        assertThat(query).contains("LIMIT 100 FOR UPDATE SKIP LOCKED")
                .contains("ORDER BY CREATED_AT ASC");

        String indexDdl = dialect.getSparseIndexDdl("ORDERS", "IDX_OUTBOX_PARTIAL", columns);
        assertThat(indexDdl).contains("WHERE OUTBOX_STATUS IN ('NEW', 'FAILED')");
    }

    @Test
    void testMySqlDialectSql() {
        MySqlDialect dialect = new MySqlDialect();
        assertThat(dialect.getDialectType()).isEqualTo(DialectType.MYSQL);
        assertThat(dialect.getLockingClause()).isEqualTo("FOR UPDATE SKIP LOCKED");
        assertThat(dialect.getTimestampFunction()).isEqualTo("NOW(6)");

        String query = dialect.buildSelectBatchForUpdateQuery("ORDERS", columns, 50, 5);
        assertThat(query).contains("LIMIT 50 FOR UPDATE SKIP LOCKED");
    }

    @Test
    void testSqlServerDialectSql() {
        SqlServerDialect dialect = new SqlServerDialect();
        assertThat(dialect.getDialectType()).isEqualTo(DialectType.MSSQL);
        assertThat(dialect.getLockingClause()).isEqualTo("WITH (UPDLOCK, READPAST, ROWLOCK)");
        assertThat(dialect.getTimestampFunction()).isEqualTo("SYSUTCDATETIME()");

        String query = dialect.buildSelectBatchForUpdateQuery("ORDERS", columns, 100, 5);
        assertThat(query).contains("SELECT TOP (100)")
                .contains("FROM ORDERS WITH (UPDLOCK, READPAST, ROWLOCK)");

        String reap = dialect.buildReapStaleQuery("ORDERS", columns, 180, 5);
        assertThat(reap).contains("DATEADD(second, -180, SYSUTCDATETIME())");
    }
}
