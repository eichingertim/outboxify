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

import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxResult;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Pluggable message broker publisher abstraction (e.g. Kafka, Redpanda, RabbitMQ).
 */
public interface BrokerPublisher extends Closeable {

    /**
     * Publishes a single outbox record asynchronously without blocking the calling thread.
     *
     * @param pipeline pipeline name
     * @param record the record to publish
     * @return CompletableFuture containing publishing outcome
     */
    CompletableFuture<OutboxResult> publish(String pipeline, OutboxRecord record);

    /**
     * Publishes a batch of outbox records concurrently.
     *
     * @param pipeline pipeline name
     * @param records list of records
     * @return CompletableFuture resolving to list of results for each record
     */
    CompletableFuture<List<OutboxResult>> publishBatch(String pipeline, List<OutboxRecord> records);

    @Override
    default void close() {
        // default no-op
    }
}
