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

import io.outboxify.core.model.DefaultOutboxRecord;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxResult;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.BrokerPublisher;
import io.outboxify.core.spi.OutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxEngineTest {

    @Mock
    private OutboxRepository repository;

    @Mock
    private BrokerPublisher brokerPublisher;

    private PipelineConfig config;
    private OutboxDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        config = PipelineConfig.builder()
                .name("orders")
                .tableName("ORDERS")
                .batchSize(10)
                .processingTimeoutSeconds(60)
                .maxRetries(3)
                .build();

        dispatcher = new OutboxDispatcher(repository, brokerPublisher);
    }

    @Test
    void testFastPathSuccess() {
        OutboxPayload payload = OutboxPayload.of("orders.v1", "order-1", "{\"id\":1}");
        when(brokerPublisher.publish(eq("orders"), any(OutboxRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(OutboxResult.success("rec-1", "orders.v1", 0, 100L)));

        CompletableFuture<OutboxResult> future = dispatcher.dispatchFastPath(config, "rec-1", payload);
        OutboxResult result = future.join();

        assertThat(result.isSuccess()).isTrue();
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() ->
                verify(repository).markSentSingle(eq(config), eq("rec-1"))
        );
    }

    @Test
    void testFastPathFailure() {
        OutboxPayload payload = OutboxPayload.of("orders.v1", "order-1", "{\"id\":1}");
        when(brokerPublisher.publish(eq("orders"), any(OutboxRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(OutboxResult.failure("rec-1", "orders.v1", "Kafka broker unavailable", null)));

        CompletableFuture<OutboxResult> future = dispatcher.dispatchFastPath(config, "rec-1", payload);
        OutboxResult result = future.join();

        assertThat(result.isSuccess()).isFalse();
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() ->
                verify(repository).markFailedSingle(eq(config), eq("rec-1"), eq("Kafka broker unavailable"))
        );
    }

    @Test
    void testPollerCycle() {
        DefaultOutboxRecord record1 = DefaultOutboxRecord.fromPayload("orders", "rec-1", OutboxPayload.of("topic1", "data1"));
        DefaultOutboxRecord record2 = DefaultOutboxRecord.fromPayload("orders", "rec-2", OutboxPayload.of("topic2", "data2"));
        List<OutboxRecord> batch = List.of(record1, record2);

        when(repository.fetchBatchForUpdate(eq(config), eq(10))).thenReturn(batch);
        when(brokerPublisher.publishBatch(eq("orders"), eq(batch)))
                .thenReturn(CompletableFuture.completedFuture(List.of(
                        OutboxResult.success("rec-1", "topic1", 0, 1L),
                        OutboxResult.success("rec-2", "topic2", 0, 2L)
                )));

        try (OutboxPollerEngine poller = new OutboxPollerEngine(config, repository, dispatcher)) {
            int processed = poller.pollOnce();
            assertThat(processed).isEqualTo(2);

            verify(repository).fetchBatchForUpdate(eq(config), eq(10));
            await().atMost(2, TimeUnit.SECONDS).untilAsserted(() ->
                    verify(repository).markSent(eq(config), eq(List.of("rec-1", "rec-2")))
            );
        }
    }

    @Test
    void testReaperJob() {
        when(repository.reapStaleRecords(eq(config), eq(60), eq(3))).thenReturn(5);

        try (OutboxReaperJob reaper = new OutboxReaperJob(config, repository)) {
            int reaped = reaper.reapOnce();
            assertThat(reaped).isEqualTo(5);

            verify(repository).reapStaleRecords(eq(config), eq(60), eq(3));
        }
    }
}
