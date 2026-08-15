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

package io.outboxify.core.engine;

import io.outboxify.core.model.DefaultOutboxRecord;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxResult;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.BrokerPublisher;
import io.outboxify.core.spi.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Coordinates non-blocking async dispatch of outbox records to the message broker
 * and transitions state in the database upon ACK / NACK.
 */
public class OutboxDispatcher {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final OutboxRepository repository;
    private final BrokerPublisher brokerPublisher;
    private final ExecutorService asyncExecutor;

    public OutboxDispatcher(OutboxRepository repository, BrokerPublisher brokerPublisher) {
        this(repository, brokerPublisher, Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "outboxify-dispatcher-" + System.identityHashCode(r));
            t.setDaemon(true);
            return t;
        }));
    }

    public OutboxDispatcher(OutboxRepository repository, BrokerPublisher brokerPublisher, ExecutorService asyncExecutor) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.brokerPublisher = Objects.requireNonNull(brokerPublisher, "brokerPublisher must not be null");
        this.asyncExecutor = Objects.requireNonNull(asyncExecutor, "asyncExecutor must not be null");
    }

    /**
     * Executes fast-path immediate send after transaction commit.
     *
     * @param config pipeline configuration
     * @param recordId record identifier
     * @param payload message payload and topic
     * @return CompletableFuture resolving to the publish result
     */
    public CompletableFuture<OutboxResult> dispatchFastPath(PipelineConfig config, String recordId, OutboxPayload payload) {
        OutboxRecord record = DefaultOutboxRecord.fromPayload(config.getName(), recordId, payload);
        log.debug("Fast-path dispatching record ID '{}' on pipeline '{}' to topic '{}'", recordId, config.getName(), payload.getTopic());

        return brokerPublisher.publish(config.getName(), record)
                .thenApplyAsync(result -> {
                    try {
                        if (result.isSuccess()) {
                            log.debug("Fast-path send succeeded for ID '{}'", recordId);
                            repository.markSentSingle(config, recordId);
                        } else {
                            log.warn("Fast-path send failed for ID '{}': {}", recordId, result.getErrorMessage());
                            repository.markFailedSingle(config, recordId, result.getErrorMessage());
                        }
                    } catch (Exception e) {
                        log.error("Failed to update status in DB for record ID '{}'", recordId, e);
                    }
                    return result;
                }, asyncExecutor)
                .exceptionally(ex -> {
                    log.error("Fast-path exception sending record ID '{}'", recordId, ex);
                    try {
                        repository.markFailedSingle(config, recordId, ex.getMessage());
                    } catch (Exception dbEx) {
                        log.error("Failed to record failure in DB for ID '{}'", recordId, dbEx);
                    }
                    return OutboxResult.failure(recordId, payload.getTopic(), ex.getMessage(), ex);
                });
    }

    /**
     * Executes slow-path batch dispatch for locked records.
     *
     * @param config pipeline configuration
     * @param records batch of locked records
     * @return CompletableFuture resolving when all records in batch are processed
     */
    public CompletableFuture<List<OutboxResult>> dispatchBatch(PipelineConfig config, List<OutboxRecord> records) {
        if (records == null || records.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }

        log.debug("Batch dispatching {} records on pipeline '{}'", records.size(), config.getName());
        return brokerPublisher.publishBatch(config.getName(), records)
                .thenApplyAsync(results -> {
                    List<String> sentIds = new ArrayList<>();
                    for (OutboxResult result : results) {
                        if (result.isSuccess()) {
                            sentIds.add(result.getRecordId());
                        } else {
                            log.warn("Record ID '{}' failed during batch send: {}", result.getRecordId(), result.getErrorMessage());
                            try {
                                repository.markFailedSingle(config, result.getRecordId(), result.getErrorMessage());
                            } catch (Exception e) {
                                log.error("Failed to mark record '{}' as FAILED", result.getRecordId(), e);
                            }
                        }
                    }

                    if (!sentIds.isEmpty()) {
                        try {
                            repository.markSent(config, sentIds);
                            log.debug("Batch marked {} records as SENT on pipeline '{}'", sentIds.size(), config.getName());
                        } catch (Exception e) {
                            log.error("Failed to batch update records to SENT on pipeline '{}'", config.getName(), e);
                        }
                    }
                    return results;
                }, asyncExecutor);
    }
}
