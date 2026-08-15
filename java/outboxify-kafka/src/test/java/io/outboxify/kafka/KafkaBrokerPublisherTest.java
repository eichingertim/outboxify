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

import io.outboxify.core.model.DefaultOutboxRecord;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaBrokerPublisherTest {

    @Mock
    private Producer<String, String> producer;

    private KafkaBrokerPublisher brokerPublisher;

    @BeforeEach
    void setUp() {
        brokerPublisher = new KafkaBrokerPublisher(producer);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPublishSuccess() {
        OutboxPayload payload = OutboxPayload.of("orders.v1", "cust-1", "{\"orderId\":100}", Map.of("x-trace-id", "trace-999"));
        DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("orders", "rec-100", payload);

        ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);

        CompletableFuture<OutboxResult> future = brokerPublisher.publish("orders", record);

        verify(producer).send(recordCaptor.capture(), callbackCaptor.capture());

        ProducerRecord<String, String> sentRecord = recordCaptor.getValue();
        assertThat(sentRecord.topic()).isEqualTo("orders.v1");
        assertThat(sentRecord.key()).isEqualTo("cust-1");
        assertThat(sentRecord.value()).isEqualTo("{\"orderId\":100}");
        assertThat(sentRecord.headers().lastHeader("x-trace-id")).isNotNull();

        // Simulate Kafka broker ACK
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("orders.v1", 2),
                0, 55, System.currentTimeMillis(), 0, 0
        );
        callbackCaptor.getValue().onCompletion(metadata, null);

        OutboxResult result = future.join();
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getPartition()).isEqualTo(2);
        assertThat(result.getOffset()).isEqualTo(55L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPublishFailure() {
        OutboxPayload payload = OutboxPayload.of("orders.v1", "cust-1", "{\"orderId\":100}");
        DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("orders", "rec-101", payload);

        ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        CompletableFuture<OutboxResult> future = brokerPublisher.publish("orders", record);

        verify(producer).send(any(ProducerRecord.class), callbackCaptor.capture());

        // Simulate broker error
        callbackCaptor.getValue().onCompletion(null, new RuntimeException("Broker leader not available"));

        OutboxResult result = future.join();
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorMessage()).isEqualTo("Broker leader not available");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testBatchPublish() {
        DefaultOutboxRecord r1 = DefaultOutboxRecord.fromPayload("orders", "1", OutboxPayload.of("t1", "p1"));
        DefaultOutboxRecord r2 = DefaultOutboxRecord.fromPayload("orders", "2", OutboxPayload.of("t2", "p2"));

        ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        CompletableFuture<List<OutboxResult>> future = brokerPublisher.publishBatch("orders", List.of(r1, r2));

        verify(producer, org.mockito.Mockito.times(2)).send(any(ProducerRecord.class), callbackCaptor.capture());

        List<Callback> callbacks = callbackCaptor.getAllValues();
        RecordMetadata md1 = new RecordMetadata(new TopicPartition("t1", 0), 0, 1, 0, 0, 0);
        RecordMetadata md2 = new RecordMetadata(new TopicPartition("t2", 1), 0, 2, 0, 0, 0);

        callbacks.get(0).onCompletion(md1, null);
        callbacks.get(1).onCompletion(md2, null);

        List<OutboxResult> results = future.join();
        assertThat(results).hasSize(2);
        assertThat(results.get(0).isSuccess()).isTrue();
        assertThat(results.get(1).isSuccess()).isTrue();
    }
}
