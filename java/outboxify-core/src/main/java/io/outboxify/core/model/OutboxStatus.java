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

/**
 * Lifecycle states of an outbox record in the transactional outbox engine.
 */
public enum OutboxStatus {
    /**
     * The record was inserted into the database within an application transaction.
     * Ready to be processed either via the fast-path commit hook or the slow-path poller.
     */
    NEW,

    /**
     * The record has been locked and claimed by a poller worker for batch dispatch to the broker.
     */
    PROCESSING,

    /**
     * The broker acknowledged receipt (e.g. Kafka ACK). Terminal successful state.
     */
    SENT,

    /**
     * Dispatch to the broker failed or transient error occurred. Eligible for retry if retry count &lt; max retries.
     */
    FAILED,

    /**
     * The record exceeded maximum retry attempts and is archived for manual inspection or dead-letter processing.
     */
    DEAD_LETTER;

    /**
     * Returns true if the status allows transition to {@link #PROCESSING}.
     *
     * @return true if eligible for pickup
     */
    public boolean isEligibleForProcessing() {
        return this == NEW || this == FAILED;
    }

    /**
     * Parses status string safely with fallback.
     *
     * @param value raw status string
     * @return matching OutboxStatus or NEW if null/unrecognized
     */
    public static OutboxStatus fromString(String value) {
        if (value == null || value.trim().isEmpty()) {
            return NEW;
        }
        try {
            return OutboxStatus.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return NEW;
        }
    }
}
