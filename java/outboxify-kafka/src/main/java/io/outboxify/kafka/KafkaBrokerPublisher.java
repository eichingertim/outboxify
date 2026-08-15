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

package io.outboxify.kafka;

import io.outboxify.core.model.BrokerConfig;
import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxResult;
import io.outboxify.core.spi.BrokerPublisher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Resilient Kafka producer wrapper implementing {@link BrokerPublisher}
 * with idempotent publishing, configurable delivery acknowledgements (acks=all),
 * and non-blocking asynchronous batch futures.
 */
public class KafkaBrokerPublisher implements BrokerPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaBrokerPublisher.class);

    private final Producer<String, String> defaultProducer;
    private final Map<String, Producer<String, String>> pipelineProducers = new ConcurrentHashMap<>();
    private final Map<String, BrokerConfig> pipelineConfigs = new ConcurrentHashMap<>();

    public KafkaBrokerPublisher(Producer<String, String> defaultProducer) {
        this.defaultProducer = Objects.requireNonNull(defaultProducer, "defaultProducer must not be null");
    }

    public KafkaBrokerPublisher(BrokerConfig defaultBrokerConfig) {
        this.defaultProducer = createProducer(defaultBrokerConfig);
    }

    public void registerPipelineBroker(String pipeline, BrokerConfig brokerConfig) {
        if (brokerConfig != null && brokerConfig.getBootstrapServers() != null) {
            pipelineConfigs.put(pipeline, brokerConfig);
            pipelineProducers.put(pipeline, createProducer(brokerConfig));
        }
    }

    public static Producer<String, String> createProducer(BrokerConfig config) {
        Properties props = new Properties();

        // Defaults
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getBootstrapServers() != null ? config.getBootstrapServers() : "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(config.isEnableIdempotence()));
        props.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(config.getLingerMs()));
        props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(config.getRetries()));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Custom additional properties
        if (config.getAdditionalProperties() != null) {
            props.putAll(config.getAdditionalProperties());
        }

        return new KafkaProducer<>(props);
    }

    private Producer<String, String> getProducer(String pipeline) {
        if (pipeline != null && pipelineProducers.containsKey(pipeline)) {
            return pipelineProducers.get(pipeline);
        }
        return defaultProducer;
    }

    @Override
    public CompletableFuture<OutboxResult> publish(String pipeline, OutboxRecord record) {
        CompletableFuture<OutboxResult> future = new CompletableFuture<>();
        Producer<String, String> producer = getProducer(pipeline);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                record.getTopic(),
                record.getPartitionKey(),
                record.getPayload()
        );

        // Add headers
        if (record.getHeaders() != null) {
            for (Map.Entry<String, String> entry : record.getHeaders().entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    producerRecord.headers().add(
                            new RecordHeader(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8))
                    );
                }
            }
        }

        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to publish outbox record ID '{}' to topic '{}': {}",
                        record.getOutboxId(), record.getTopic(), exception.getMessage());
                future.complete(OutboxResult.failure(
                        record.getOutboxId(),
                        record.getTopic(),
                        exception.getMessage(),
                        exception
                ));
            } else {
                log.debug("Successfully published record ID '{}' to topic '{}' [partition={}, offset={}]",
                        record.getOutboxId(), metadata.topic(), metadata.partition(), metadata.offset());
                future.complete(OutboxResult.success(
                        record.getOutboxId(),
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                ));
            }
        });

        return future;
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
        try {
            defaultProducer.close();
        } catch (Exception e) {
            log.warn("Error closing default Kafka producer: {}", e.getMessage());
        }

        for (Producer<String, String> producer : pipelineProducers.values()) {
            try {
                producer.close();
            } catch (Exception e) {
                log.warn("Error closing pipeline Kafka producer: {}", e.getMessage());
            }
        }
        pipelineProducers.clear();
    }
}
