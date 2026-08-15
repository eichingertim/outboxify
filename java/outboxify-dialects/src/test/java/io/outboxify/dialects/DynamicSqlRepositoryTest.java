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

import io.outboxify.core.model.DefaultOutboxRecord;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxStatus;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.DialectType;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class DynamicSqlRepositoryTest {

    private JdbcDataSource dataSource;
    private DialectRegistry dialectRegistry;
    private DynamicSqlRepository repository;
    private PipelineConfig config;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:outbox_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        dataSource.setUser("sa");
        dataSource.setPassword("");

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE OUTBOX_RECORD (" +
                    "id VARCHAR(64) PRIMARY KEY, " +
                    "topic VARCHAR(255) NOT NULL, " +
                    "partition_key VARCHAR(255), " +
                    "payload TEXT NOT NULL, " +
                    "headers TEXT, " +
                    "status VARCHAR(32) NOT NULL, " +
                    "retry_count INT DEFAULT 0, " +
                    "last_error TEXT, " +
                    "created_at TIMESTAMP NOT NULL, " +
                    "updated_at TIMESTAMP NOT NULL, " +
                    "processed_at TIMESTAMP)");
        }

        dialectRegistry = new DialectRegistry();
        repository = new DynamicSqlRepository(dataSource, dialectRegistry);

        config = PipelineConfig.builder()
                .name("default")
                .tableName("OUTBOX_RECORD")
                .dialect(DialectType.H2)
                .batchSize(10)
                .processingTimeoutSeconds(5)
                .maxRetries(3)
                .build();
    }

    @Test
    void testInsertAndFetchBatch() {
        OutboxPayload payload = OutboxPayload.of("events.v1", "user-42", "{\"action\":\"register\"}", Map.of("correlationId", "c-1"));
        DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("default", "rec-1", payload);

        String id = repository.insertRecord(config, record);
        assertThat(id).isEqualTo("rec-1");

        Optional<OutboxRecord> found = repository.findById(config, "rec-1");
        assertThat(found).isPresent();
        assertThat(found.get().getTopic()).isEqualTo("events.v1");
        assertThat(found.get().getPartitionKey()).isEqualTo("user-42");
        assertThat(found.get().getStatus()).isEqualTo(OutboxStatus.NEW);
        assertThat(found.get().getHeaders()).containsEntry("correlationId", "c-1");

        List<OutboxRecord> batch = repository.fetchBatchForUpdate(config, 10);
        assertThat(batch).hasSize(1);
        assertThat(batch.get(0).getOutboxId()).isEqualTo("rec-1");
    }

    @Test
    void testStateTransitionsLifecycle() {
        OutboxPayload payload = OutboxPayload.of("orders.v1", "{\"orderId\":\"101\"}");
        DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("default", "rec-2", payload);
        repository.insertRecord(config, record);

        // 1. Mark PROCESSING
        int processingCount = repository.markProcessing(config, List.of("rec-2"));
        assertThat(processingCount).isEqualTo(1);
        assertThat(repository.findById(config, "rec-2").get().getStatus()).isEqualTo(OutboxStatus.PROCESSING);

        // 2. Mark SENT
        int sentCount = repository.markSent(config, List.of("rec-2"));
        assertThat(sentCount).isEqualTo(1);
        OutboxRecord sentRecord = repository.findById(config, "rec-2").get();
        assertThat(sentRecord.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(sentRecord.getProcessedAt()).isNotNull();

        // 3. Mark FAILED on another record
        DefaultOutboxRecord record3 = DefaultOutboxRecord.fromPayload("default", "rec-3", payload);
        repository.insertRecord(config, record3);
        repository.markFailed(config, List.of("rec-3"), "Connection timed out");
        OutboxRecord failedRecord = repository.findById(config, "rec-3").get();
        assertThat(failedRecord.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(failedRecord.getRetryCount()).isEqualTo(1);
        assertThat(failedRecord.getLastError()).isEqualTo("Connection timed out");
    }

    @Test
    void testReapStaleRecords() throws Exception {
        OutboxPayload payload = OutboxPayload.of("orders.v1", "{\"orderId\":\"102\"}");
        DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("default", "rec-4", payload);
        repository.insertRecord(config, record);
        repository.markProcessing(config, List.of("rec-4"));

        // Simulate stale updated_at in past
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE OUTBOX_RECORD SET updated_at = DATEADD('SECOND', -10, CURRENT_TIMESTAMP) WHERE id = 'rec-4'");
        }

        int reaped = repository.reapStaleRecords(config, 5, 3);
        assertThat(reaped).isEqualTo(1);

        OutboxRecord reapedRecord = repository.findById(config, "rec-4").get();
        assertThat(reapedRecord.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(reapedRecord.getRetryCount()).isEqualTo(1);
    }
}
