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

package io.outboxify.spring.publisher;

import io.outboxify.core.engine.OutboxDispatcher;
import io.outboxify.core.engine.OutboxHook;
import io.outboxify.core.engine.OutboxPublisher;
import io.outboxify.core.model.DefaultOutboxRecord;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxResult;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Spring-managed implementation of {@link OutboxPublisher} supporting transactional staging
 * and fast-path asynchronous dispatch upon transaction commit.
 */
public class SpringOutboxPublisher implements OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(SpringOutboxPublisher.class);

    private final OutboxRepository repository;
    private final OutboxHook hook;
    private final OutboxDispatcher dispatcher;
    private final Function<String, PipelineConfig> pipelineConfigResolver;

    public SpringOutboxPublisher(OutboxRepository repository,
                                 OutboxHook hook,
                                 OutboxDispatcher dispatcher,
                                 Function<String, PipelineConfig> pipelineConfigResolver) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.hook = Objects.requireNonNull(hook, "hook must not be null");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
        this.pipelineConfigResolver = Objects.requireNonNull(pipelineConfigResolver, "pipelineConfigResolver must not be null");
    }

    @Override
    public String stage(String pipeline, OutboxPayload message) {
        PipelineConfig config = getRequiredPipeline(pipeline);
        String recordId = UUID.randomUUID().toString();
        DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload(config.getName(), recordId, message);

        repository.insertRecord(config, record);
        log.debug("Staged outbox record ID '{}' into table '{}' on pipeline '{}'", recordId, config.getTableName(), pipeline);
        return recordId;
    }

    @Override
    public CompletableFuture<OutboxResult> publish(String pipeline, OutboxPayload message) {
        PipelineConfig config = getRequiredPipeline(pipeline);
        String recordId = stage(pipeline, message);

        if (config.isImmediateSendEnabled()) {
            hook.registerForCommit(pipeline, recordId, message);
        }

        return CompletableFuture.completedFuture(OutboxResult.success(recordId, message.getTopic(), 0, 0));
    }

    private PipelineConfig getRequiredPipeline(String pipeline) {
        PipelineConfig config = pipelineConfigResolver.apply(pipeline != null ? pipeline : "default");
        if (config == null) {
            throw new IllegalArgumentException("Unknown or unconfigured Outboxify pipeline: '" + pipeline + "'");
        }
        return config;
    }
}
