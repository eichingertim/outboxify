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

package io.outboxify.spring.hook;

import io.outboxify.core.engine.OutboxDispatcher;
import io.outboxify.core.engine.OutboxHook;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.PipelineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Fast-path outbox hook integrating with Spring's {@link TransactionSynchronizationManager}.
 * Guarantees that broker publishing occurs strictly AFTER transaction commit (zero phantom sends).
 */
public class TransactionalOutboxHook implements OutboxHook {

    private static final Logger log = LoggerFactory.getLogger(TransactionalOutboxHook.class);

    private final OutboxDispatcher dispatcher;
    private final Function<String, PipelineConfig> pipelineConfigResolver;

    public TransactionalOutboxHook(OutboxDispatcher dispatcher, Function<String, PipelineConfig> pipelineConfigResolver) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
        this.pipelineConfigResolver = Objects.requireNonNull(pipelineConfigResolver, "pipelineConfigResolver must not be null");
    }

    @Override
    public void registerForCommit(String pipeline, String recordId, OutboxPayload message) {
        PipelineConfig config = pipelineConfigResolver.apply(pipeline);
        if (config == null || !config.isEnabled() || !config.isImmediateSendEnabled()) {
            log.trace("Immediate fast-path dispatch disabled for pipeline '{}', deferred to poller", pipeline);
            return;
        }

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            log.debug("Registering transaction synchronization hook for record ID '{}' on pipeline '{}'", recordId, pipeline);
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    log.debug("Transaction committed. Triggering fast-path dispatch for record ID '{}'", recordId);
                    dispatcher.dispatchFastPath(config, recordId, message);
                }
            });
        } else {
            log.debug("No active transaction synchronization found; dispatching fast-path immediately for record ID '{}'", recordId);
            dispatcher.dispatchFastPath(config, recordId, message);
        }
    }
}
