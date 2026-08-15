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

package io.outboxify.spring.it.containers;

import io.outboxify.core.model.DefaultOutboxRecord;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxStatus;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.DialectType;
import io.outboxify.dialects.DialectRegistry;
import io.outboxify.dialects.DynamicSqlRepository;
import oracle.jdbc.pool.OracleDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class OracleTestcontainersIT {

    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23-slim-faststart");

    static {
        oracle.start();
    }

    private static DynamicSqlRepository repository;
    private static PipelineConfig config;

    @BeforeAll
    static void setUpOracle() throws Exception {
        OracleDataSource ds = new OracleDataSource();
        ds.setURL(oracle.getJdbcUrl());
        ds.setUser(oracle.getUsername());
        ds.setPassword(oracle.getPassword());

        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE OUTBOX_ORACLE (" +
                    "id VARCHAR2(64) PRIMARY KEY, " +
                    "topic VARCHAR2(255) NOT NULL, " +
                    "partition_key VARCHAR2(255), " +
                    "payload CLOB NOT NULL, " +
                    "headers VARCHAR2(4000), " +
                    "status VARCHAR2(32) NOT NULL, " +
                    "retry_count NUMBER DEFAULT 0, " +
                    "last_error VARCHAR2(4000), " +
                    "created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL, " +
                    "updated_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL, " +
                    "processed_at TIMESTAMP)");

            // Oracle Function-Based Index for Sparse Status Filtering
            stmt.execute("CREATE INDEX idx_outbox_oracle_sparse ON OUTBOX_ORACLE (" +
                    "CASE WHEN status IN ('NEW', 'FAILED') THEN status ELSE NULL END, created_at)");
        }

        DialectRegistry registry = new DialectRegistry();
        repository = new DynamicSqlRepository(ds, registry);

        config = PipelineConfig.builder()
                .name("oracle-pipeline")
                .tableName("OUTBOX_ORACLE")
                .dialect(DialectType.ORACLE)
                .batchSize(100)
                .processingTimeoutSeconds(180)
                .maxRetries(5)
                .build();
    }

    @Test
    void testOracleDialect_SkipLockedAndPaging() {
        // Seed records
        OutboxPayload p1 = OutboxPayload.of("oracle.topic", "key-1", "{\"msg\":1}");
        OutboxPayload p2 = OutboxPayload.of("oracle.topic", "key-2", "{\"msg\":2}");

        String id1 = repository.insertRecord(config, DefaultOutboxRecord.fromPayload("oracle-pipeline", "ora-1", p1));
        String id2 = repository.insertRecord(config, DefaultOutboxRecord.fromPayload("oracle-pipeline", "ora-2", p2));

        assertThat(id1).isEqualTo("ora-1");
        assertThat(id2).isEqualTo("ora-2");

        // Fetch batch with SKIP LOCKED and ROWNUM <= batchSize
        List<OutboxRecord> batch = repository.fetchBatchForUpdate(config, 10);
        assertThat(batch).hasSize(2);
        assertThat(batch.get(0).getStatus()).isEqualTo(OutboxStatus.NEW);

        // Verify marked PROCESSING atomically
        Optional<OutboxRecord> r1 = repository.findById(config, "ora-1");
        assertThat(r1).isPresent();
        assertThat(r1.get().getStatus()).isEqualTo(OutboxStatus.PROCESSING);

        // Mark SENT
        repository.markSent(config, List.of("ora-1", "ora-2"));
        assertThat(repository.findById(config, "ora-1").get().getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(repository.findById(config, "ora-2").get().getStatus()).isEqualTo(OutboxStatus.SENT);
    }
}
