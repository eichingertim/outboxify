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

package io.outboxify.core.model;

import io.outboxify.core.spi.DialectType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxModelTest {

    @Test
    void testOutboxStatusTransitions() {
        assertThat(OutboxStatus.NEW.isEligibleForProcessing()).isTrue();
        assertThat(OutboxStatus.FAILED.isEligibleForProcessing()).isTrue();
        assertThat(OutboxStatus.PROCESSING.isEligibleForProcessing()).isFalse();
        assertThat(OutboxStatus.SENT.isEligibleForProcessing()).isFalse();
        assertThat(OutboxStatus.DEAD_LETTER.isEligibleForProcessing()).isFalse();

        assertThat(OutboxStatus.fromString("PROCESSING")).isEqualTo(OutboxStatus.PROCESSING);
        assertThat(OutboxStatus.fromString("unknown")).isEqualTo(OutboxStatus.NEW);
        assertThat(OutboxStatus.fromString(null)).isEqualTo(OutboxStatus.NEW);
    }

    @Test
    void testDefaultOutboxRecordBuilder() {
        OutboxPayload payload = OutboxPayload.builder()
                .topic("test.topic")
                .partitionKey("user-123")
                .payload("{\"event\":\"created\"}")
                .header("traceId", "abc-123")
                .build();

        DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("orders", "rec-001", payload);

        assertThat(record.getOutboxPipeline()).isEqualTo("orders");
        assertThat(record.getOutboxId()).isEqualTo("rec-001");
        assertThat(record.getTopic()).isEqualTo("test.topic");
        assertThat(record.getPartitionKey()).isEqualTo("user-123");
        assertThat(record.getPayload()).isEqualTo("{\"event\":\"created\"}");
        assertThat(record.getHeaders()).containsEntry("traceId", "abc-123");
        assertThat(record.getStatus()).isEqualTo(OutboxStatus.NEW);
        assertThat(record.getRetryCount()).isEqualTo(0);
        assertThat(record.getCreatedAt()).isNotNull();
    }

    @Test
    void testPipelineConfigDefaults() {
        PipelineConfig config = PipelineConfig.builder()
                .name("orders")
                .tableName("ORDERS")
                .dialect(DialectType.POSTGRESQL)
                .build();

        assertThat(config.getName()).isEqualTo("orders");
        assertThat(config.getTableName()).isEqualTo("ORDERS");
        assertThat(config.getDialect()).isEqualTo(DialectType.POSTGRESQL);
        assertThat(config.getBatchSize()).isEqualTo(100);
        assertThat(config.getPollIntervalMs()).isEqualTo(1000L);
        assertThat(config.getProcessingTimeoutSeconds()).isEqualTo(300);
        assertThat(config.getMaxRetries()).isEqualTo(5);
        assertThat(config.isImmediateSendEnabled()).isTrue();
        assertThat(config.getColumns().getId()).isEqualTo("id");
    }

    @Test
    void testCustomColumnMapping() {
        ColumnMapping mapping = ColumnMapping.builder()
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

        assertThat(mapping.getId()).isEqualTo("ORDER_ID");
        assertThat(mapping.getTopic()).isEqualTo("KAFKA_TOPIC");
        assertThat(mapping.getPartitionKey()).isEqualTo("CUSTOMER_ID");
        assertThat(mapping.getPayload()).isEqualTo("OUTBOX_PAYLOAD");
        assertThat(mapping.getStatus()).isEqualTo("OUTBOX_STATUS");
    }
}
