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

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * Universal contract representing a transactional outbox record.
 * Can be implemented directly by domain entities (e.g. JPA @Entity Order)
 * or by dedicated outbox table models.
 */
public interface OutboxRecord {

    /**
     * The pipeline identifier this record belongs to.
     * Defaults to "default" if omitted.
     *
     * @return pipeline name
     */
    default String getOutboxPipeline() {
        return "default";
    }

    /**
     * Unique identifier for this outbox row (e.g. UUID or sequence ID).
     *
     * @return unique record ID
     */
    String getOutboxId();

    /**
     * Destination message broker topic (e.g. "orders.v1").
     *
     * @return target topic
     */
    String getTopic();

    /**
     * Message partition key for ordering guarantees in Kafka / partitioned brokers.
     *
     * @return partition key, or null if unkeyed
     */
    default String getPartitionKey() {
        return null;
    }

    /**
     * Serialized message payload (JSON, Avro string, or raw text).
     *
     * @return string payload
     */
    String getPayload();

    /**
     * Metadata headers dispatched with the message (tracing, event type, correlation ID).
     *
     * @return map of string key-value header pairs
     */
    default Map<String, String> getHeaders() {
        return Collections.emptyMap();
    }

    /**
     * Current lifecycle status of the record.
     *
     * @return outbox status
     */
    default OutboxStatus getStatus() {
        return OutboxStatus.NEW;
    }

    /**
     * Number of failed delivery attempts.
     *
     * @return retry count
     */
    default int getRetryCount() {
        return 0;
    }

    /**
     * Error message or stack trace summary from the last failed attempt.
     *
     * @return last error string, or null
     */
    default String getLastError() {
        return null;
    }

    /**
     * Timestamp when the record was created.
     *
     * @return creation instant
     */
    default Instant getCreatedAt() {
        return Instant.now();
    }

    /**
     * Timestamp when the record was last updated or locked.
     *
     * @return update instant
     */
    default Instant getUpdatedAt() {
        return Instant.now();
    }

    /**
     * Timestamp when the record was confirmed SENT by the message broker.
     *
     * @return processed instant, or null if pending
     */
    default Instant getProcessedAt() {
        return null;
    }
}
