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

package io.outboxify.spring.kafka;

import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxResult;
import io.outboxify.core.spi.BrokerPublisher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * {@link BrokerPublisher} implementation backed by Spring Kafka's {@link KafkaTemplate}.
 * <p>
 * Supports both single-template setups and per-pipeline template routing, seamlessly
 * integrating with Spring Boot's native Kafka configuration, micrometer observation,
 * custom interceptors, and transactions.
 */
public class SpringKafkaBrokerPublisher implements BrokerPublisher {

    private static final Logger log = LoggerFactory.getLogger(SpringKafkaBrokerPublisher.class);

    private final KafkaTemplate<?, ?> defaultTemplate;
    private final Map<String, KafkaTemplate<?, ?>> pipelineTemplates = new ConcurrentHashMap<>();

    /**
     * Constructs a publisher using the specified default {@link KafkaTemplate}.
     *
     * @param defaultTemplate the primary Kafka template to publish outbox records
     */
    public SpringKafkaBrokerPublisher(KafkaTemplate<?, ?> defaultTemplate) {
        this.defaultTemplate = Objects.requireNonNull(defaultTemplate, "defaultTemplate must not be null");
    }

    /**
     * Registers a specific {@link KafkaTemplate} to handle dispatching for a designated pipeline.
     *
     * @param pipeline the pipeline name
     * @param template the Kafka template for the pipeline
     */
    public void registerPipelineTemplate(String pipeline, KafkaTemplate<?, ?> template) {
        if (pipeline != null && template != null) {
            pipelineTemplates.put(pipeline, template);
        }
    }

    /**
     * Resolves the {@link KafkaTemplate} for a given pipeline name.
     *
     * @param pipeline the pipeline name
     * @return the pipeline-specific template, or the default template
     */
    public KafkaTemplate<?, ?> getTemplate(String pipeline) {
        if (pipeline != null && pipelineTemplates.containsKey(pipeline)) {
            return pipelineTemplates.get(pipeline);
        }
        return defaultTemplate;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public CompletableFuture<OutboxResult> publish(String pipeline, OutboxRecord record) {
        CompletableFuture<OutboxResult> resultFuture = new CompletableFuture<>();
        KafkaTemplate template = getTemplate(pipeline);

        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(
                record.getTopic(),
                record.getPartitionKey(),
                record.getPayload()
        );

        if (record.getHeaders() != null) {
            for (Map.Entry<String, String> entry : record.getHeaders().entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    producerRecord.headers().add(
                            new RecordHeader(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8))
                    );
                }
            }
        }

        try {
            CompletableFuture<?> sendFuture = template.send(producerRecord);
            sendFuture.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish outbox record ID '{}' to topic '{}' via KafkaTemplate: {}",
                            record.getOutboxId(), record.getTopic(), ex.getMessage());
                    resultFuture.complete(OutboxResult.failure(
                            record.getOutboxId(),
                            record.getTopic(),
                            ex.getMessage(),
                            ex instanceof Exception ? (Exception) ex : new RuntimeException(ex)
                    ));
                } else if (result instanceof SendResult sendResult) {
                    RecordMetadata metadata = sendResult.getRecordMetadata();
                    log.debug("Successfully published record ID '{}' to topic '{}' [partition={}, offset={}] via KafkaTemplate",
                            record.getOutboxId(), metadata.topic(), metadata.partition(), metadata.offset());
                    resultFuture.complete(OutboxResult.success(
                            record.getOutboxId(),
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset()
                    ));
                } else {
                    resultFuture.complete(OutboxResult.success(
                            record.getOutboxId(),
                            record.getTopic(),
                            -1,
                            -1L
                    ));
                }
            });
        } catch (Exception ex) {
            log.error("Immediate failure sending outbox record ID '{}' to topic '{}': {}",
                    record.getOutboxId(), record.getTopic(), ex.getMessage());
            resultFuture.complete(OutboxResult.failure(
                    record.getOutboxId(),
                    record.getTopic(),
                    ex.getMessage(),
                    ex
            ));
        }

        return resultFuture;
    }

    @Override
    public CompletableFuture<List<OutboxResult>> publishBatch(String pipeline, List<OutboxRecord> records) {
        if (records == null || records.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }

        List<CompletableFuture<OutboxResult>> futures = records.stream()
                .map(record -> publish(pipeline, record))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }

    @Override
    public void close() {
        // Lifecycle of KafkaTemplate is managed by Spring ApplicationContext / ProducerFactory
        pipelineTemplates.clear();
    }
}
