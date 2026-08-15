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
import io.outboxify.core.model.PipelineConfig;

import java.util.List;
import java.util.Optional;

/**
 * Storage SPI for reading and updating outbox record lifecycle states in the relational database.
 */
public interface OutboxRepository {

    /**
     * Atomically selects, locks (SKIP LOCKED), and claims a batch of eligible records into PROCESSING status
     * in a single isolated transaction to eliminate duplicate delivery across concurrent workers.
     *
     * @param config pipeline configuration
     * @param batchSize maximum records to fetch
     * @return list of claimed records in PROCESSING state
     */
    List<OutboxRecord> fetchBatchForUpdate(PipelineConfig config, int batchSize);

    /**
     * Transitions a batch of records to PROCESSING status.
     *
     * @param config pipeline configuration
     * @param recordIds record identifiers
     * @return number of affected rows
     */
    int markProcessing(PipelineConfig config, List<String> recordIds);

    /**
     * Transitions a batch of records to SENT status upon successful broker ACK.
     *
     * @param config pipeline configuration
     * @param recordIds record identifiers
     * @return number of affected rows
     */
    int markSent(PipelineConfig config, List<String> recordIds);

    /**
     * Transitions a single record to SENT status.
     *
     * @param config pipeline configuration
     * @param recordId record identifier
     * @return number of affected rows
     */
    int markSentSingle(PipelineConfig config, String recordId);

    /**
     * Transitions a batch of records to FAILED status and increments retry counts.
     *
     * @param config pipeline configuration
     * @param recordIds record identifiers
     * @param errorMessage error description
     * @return number of affected rows
     */
    int markFailed(PipelineConfig config, List<String> recordIds, String errorMessage);

    /**
     * Transitions a single record to FAILED status and increments retry count.
     *
     * @param config pipeline configuration
     * @param recordId record identifier
     * @param errorMessage error description
     * @return number of affected rows
     */
    int markFailedSingle(PipelineConfig config, String recordId, String errorMessage);

    /**
     * Reaps stale PROCESSING rows whose age exceeds the timeout, resetting them to FAILED or NEW.
     *
     * @param config pipeline configuration
     * @param timeoutSeconds timeout threshold in seconds
     * @param maxRetries retry ceiling
     * @return number of reaped rows
     */
    int reapStaleRecords(PipelineConfig config, int timeoutSeconds, int maxRetries);

    /**
     * Persists a new outbox record into the database table.
     *
     * @param config pipeline configuration
     * @param record record to persist
     * @return generated or persisted record ID
     */
    String insertRecord(PipelineConfig config, OutboxRecord record);

    /**
     * Retrieves an outbox record by ID.
     *
     * @param config pipeline configuration
     * @param recordId record identifier
     * @return optional containing the record if found
     */
    Optional<OutboxRecord> findById(PipelineConfig config, String recordId);
}
