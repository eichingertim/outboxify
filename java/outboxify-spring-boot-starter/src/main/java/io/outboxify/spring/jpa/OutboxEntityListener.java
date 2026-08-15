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

package io.outboxify.spring.jpa;

import io.outboxify.core.engine.OutboxHook;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxRecord;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PostUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JPA EntityListener intercepting persistence events on domain entities implementing {@link OutboxRecord}.
 * Automatically registers commit hooks for seamless fast-path dispatch.
 */
public class OutboxEntityListener {

    private static final Logger log = LoggerFactory.getLogger(OutboxEntityListener.class);

    private static volatile OutboxHook outboxHook;

    public static void setOutboxHook(OutboxHook hook) {
        outboxHook = hook;
    }

    @PostPersist
    @PostUpdate
    public void onPostPersistOrUpdate(Object target) {
        if (target instanceof OutboxRecord record) {
            if (outboxHook == null) {
                log.trace("OutboxHook is not configured, skipping JPA entity interceptor");
                return;
            }

            String pipeline = record.getOutboxPipeline() != null ? record.getOutboxPipeline() : "default";
            String recordId = record.getOutboxId();
            if (recordId == null || record.getTopic() == null || record.getPayload() == null) {
                log.trace("OutboxRecord entity is missing required fields (id, topic, or payload), skipping hook");
                return;
            }

            OutboxPayload payload = OutboxPayload.builder()
                    .topic(record.getTopic())
                    .partitionKey(record.getPartitionKey())
                    .payload(record.getPayload())
                    .headers(record.getHeaders())
                    .build();

            log.debug("JPA EntityListener intercepted record ID '{}' on pipeline '{}'", recordId, pipeline);
            outboxHook.registerForCommit(pipeline, recordId, payload);
        }
    }
}
