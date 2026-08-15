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

import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxResult;

import java.util.concurrent.CompletableFuture;

/**
 * Public programmatic API for staging and publishing outbox messages.
 */
public interface OutboxPublisher {

    /**
     * Stages an outbox record into the database table within the current database transaction.
     * The record will be picked up by the slow-path poller or fast-path commit hook.
     *
     * @param pipeline pipeline name (e.g., "orders")
     * @param message payload and destination topic
     * @return persisted outbox record identifier
     */
    String stage(String pipeline, OutboxPayload message);

    /**
     * Stages the record and dispatches it via the fast-path commit hook upon transaction completion.
     *
     * @param pipeline pipeline name
     * @param message payload and destination topic
     * @return CompletableFuture resolving when the broker confirms receipt
     */
    CompletableFuture<OutboxResult> publish(String pipeline, OutboxPayload message);
}
