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

import io.outboxify.core.model.DefaultOutboxRecord;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SpringKafkaBrokerPublisherTest {

    @Mock
    private KafkaTemplate<Object, Object> defaultTemplate;

    @Mock
    private KafkaTemplate<Object, Object> ordersTemplate;

    private SpringKafkaBrokerPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new SpringKafkaBrokerPublisher(defaultTemplate);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPublishSuccess() {
        OutboxPayload payload = OutboxPayload.of("order-events", "order-123", "{\"id\":\"order-123\"}", Map.of("traceId", "trace-abc", "eventType", "Created"));
        DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("default", UUID.randomUUID().toString(), payload);

        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("order-events", 0),
                0, 0, System.currentTimeMillis(), 0, 0
        );
        SendResult<Object, Object> sendResult = new SendResult<>(null, metadata);
        CompletableFuture<SendResult<Object, Object>> sendFuture = CompletableFuture.completedFuture(sendResult);

        when(defaultTemplate.send(any(ProducerRecord.class))).thenReturn(sendFuture);

        CompletableFuture<OutboxResult> resultFuture = publisher.publish("default", record);
        OutboxResult result = resultFuture.join();

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getRecordId()).isEqualTo(record.getOutboxId());
        assertThat(result.getTopic()).isEqualTo("order-events");
        assertThat(result.getPartition()).isEqualTo(0);
        assertThat(result.getOffset()).isEqualTo(0L);

        ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(defaultTemplate).send(captor.capture());
        ProducerRecord<Object, Object> capturedRecord = captor.getValue();
        assertThat(capturedRecord.topic()).isEqualTo("order-events");
        assertThat(capturedRecord.key()).isEqualTo("order-123");
        assertThat(capturedRecord.value()).isEqualTo("{\"id\":\"order-123\"}");
        assertThat(capturedRecord.headers().lastHeader("traceId")).isNotNull();
        assertThat(new String(capturedRecord.headers().lastHeader("traceId").value())).isEqualTo("trace-abc");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPublishFailure() {
        OutboxPayload payload = OutboxPayload.of("order-events", "order-123", "{\"id\":\"order-123\"}");
        DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("default", UUID.randomUUID().toString(), payload);

        CompletableFuture<SendResult<Object, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker connection timeout"));

        when(defaultTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        CompletableFuture<OutboxResult> resultFuture = publisher.publish("default", record);
        OutboxResult result = resultFuture.join();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getRecordId()).isEqualTo(record.getOutboxId());
        assertThat(result.getTopic()).isEqualTo("order-events");
        assertThat(result.getErrorMessage()).contains("Broker connection timeout");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPublishImmediateException() {
        OutboxPayload payload = OutboxPayload.of("order-events", "order-123", "{\"id\":\"order-123\"}");
        DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("default", UUID.randomUUID().toString(), payload);

        when(defaultTemplate.send(any(ProducerRecord.class))).thenThrow(new IllegalStateException("Template closed"));

        CompletableFuture<OutboxResult> resultFuture = publisher.publish("default", record);
        OutboxResult result = resultFuture.join();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorMessage()).contains("Template closed");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPipelineSpecificTemplateRouting() {
        publisher.registerPipelineTemplate("orders", ordersTemplate);

        DefaultOutboxRecord defaultRecord = DefaultOutboxRecord.fromPayload("payments", "p-1", OutboxPayload.of("payments", "p-1", "{}"));
        DefaultOutboxRecord ordersRecord = DefaultOutboxRecord.fromPayload("orders", "o-1", OutboxPayload.of("orders", "o-1", "{}"));

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0L, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(null, metadata);

        when(defaultTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(sendResult));
        when(ordersTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(sendResult));

        publisher.publish("payments", defaultRecord).join();
        verify(defaultTemplate, times(1)).send(any(ProducerRecord.class));
        verify(ordersTemplate, never()).send(any(ProducerRecord.class));

        publisher.publish("orders", ordersRecord).join();
        verify(ordersTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPublishBatch() {
        DefaultOutboxRecord r1 = DefaultOutboxRecord.fromPayload("default", "1", OutboxPayload.of("t", "p1"));
        DefaultOutboxRecord r2 = DefaultOutboxRecord.fromPayload("default", "2", OutboxPayload.of("t", "p2"));

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("t", 0), 0, 0, 0L, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(null, metadata);

        when(defaultTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(sendResult));

        List<OutboxResult> results = publisher.publishBatch("default", List.of(r1, r2)).join();

        assertThat(results).hasSize(2);
        assertThat(results.get(0).isSuccess()).isTrue();
        assertThat(results.get(1).isSuccess()).isTrue();
        verify(defaultTemplate, times(2)).send(any(ProducerRecord.class));
    }
}
