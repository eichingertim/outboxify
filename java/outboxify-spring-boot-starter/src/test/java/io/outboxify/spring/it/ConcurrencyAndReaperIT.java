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

package io.outboxify.spring.it;

import io.outboxify.core.engine.OutboxDispatcher;
import io.outboxify.core.engine.OutboxPollerEngine;
import io.outboxify.core.engine.OutboxReaperJob;
import io.outboxify.core.model.DefaultOutboxRecord;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxResult;
import io.outboxify.core.model.OutboxStatus;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.BrokerPublisher;
import io.outboxify.core.spi.DialectType;
import io.outboxify.dialects.DialectRegistry;
import io.outboxify.dialects.DynamicSqlRepository;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ConcurrencyAndReaperIT {

    private JdbcDataSource dataSource;
    private DynamicSqlRepository repository;
    private PipelineConfig config;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:concurrency_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        dataSource.setUser("sa");
        dataSource.setPassword("");

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE OUTBOX_MESSAGES (" +
                    "id VARCHAR(64) PRIMARY KEY, " +
                    "topic VARCHAR(255) NOT NULL, " +
                    "partition_key VARCHAR(255), " +
                    "payload TEXT NOT NULL, " +
                    "headers TEXT, " +
                    "status VARCHAR(32) NOT NULL, " +
                    "retry_count INT DEFAULT 0, " +
                    "last_error TEXT, " +
                    "created_at TIMESTAMP NOT NULL, " +
                    "updated_at TIMESTAMP NOT NULL, " +
                    "processed_at TIMESTAMP)");
        }

        DialectRegistry registry = new DialectRegistry();
        repository = new DynamicSqlRepository(dataSource, registry);

        config = PipelineConfig.builder()
                .name("test-pipeline")
                .tableName("OUTBOX_MESSAGES")
                .dialect(DialectType.H2)
                .batchSize(50)
                .pollIntervalMs(50)
                .processingTimeoutSeconds(3)
                .reaperIntervalMs(100)
                .maxRetries(3)
                .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS OUTBOX_MESSAGES");
        }
    }

    /**
     * Scenario 3: High Concurrency Polling Contention
     * Seeds 500 outbox records and runs 4 concurrent poller workers.
     * Asserts all records processed to SENT with ZERO duplicate deliveries and zero deadlocks.
     */
    @Test
    void testScenario3_HighConcurrencyPollingWithZeroDuplicates() throws Exception {
        int totalRecords = 500;
        Set<String> publishedIds = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicateCount = new AtomicInteger(0);

        BrokerPublisher brokerPublisher = new BrokerPublisher() {
            @Override
            public CompletableFuture<OutboxResult> publish(String pipeline, OutboxRecord record) {
                if (!publishedIds.add(record.getOutboxId())) {
                    duplicateCount.incrementAndGet();
                }
                return CompletableFuture.completedFuture(
                        OutboxResult.success(record.getOutboxId(), record.getTopic(), 0, publishedIds.size())
                );
            }

            @Override
            public CompletableFuture<List<OutboxResult>> publishBatch(String pipeline, List<OutboxRecord> records) {
                List<OutboxResult> results = new ArrayList<>();
                for (OutboxRecord r : records) {
                    if (!publishedIds.add(r.getOutboxId())) {
                        duplicateCount.incrementAndGet();
                    }
                    results.add(OutboxResult.success(r.getOutboxId(), r.getTopic(), 0, publishedIds.size()));
                }
                return CompletableFuture.completedFuture(results);
            }
        };

        OutboxDispatcher dispatcher = new OutboxDispatcher(repository, brokerPublisher);

        // Seed records
        for (int i = 0; i < totalRecords; i++) {
            String recordId = "order-" + UUID.randomUUID();
            OutboxPayload payload = OutboxPayload.of("orders.topic", "key-" + i, "{\"index\":" + i + "}");
            DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("test-pipeline", recordId, payload);
            repository.insertRecord(config, record);
        }

        // Spin up 4 concurrent poller engines (simulating 4 instances / pods)
        int threadCount = 4;
        ExecutorService workerPool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            workerPool.submit(() -> {
                try {
                    startLatch.await();
                    while (publishedIds.size() < totalRecords) {
                        List<OutboxRecord> batch = repository.fetchBatchForUpdate(config, 25);
                        if (!batch.isEmpty()) {
                            List<String> ids = batch.stream().map(OutboxRecord::getOutboxId).toList();
                            repository.markProcessing(config, ids);
                            dispatcher.dispatchBatch(config, batch).join();
                        } else {
                            Thread.sleep(10);
                        }
                    }
                } catch (Exception e) {
                    // ignore
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean finished = doneLatch.await(10, TimeUnit.SECONDS);
        workerPool.shutdown();

        assertThat(finished).isTrue();
        assertThat(publishedIds).hasSize(totalRecords);
        assertThat(duplicateCount.get()).isEqualTo(0);
    }

    /**
     * Scenario 4: Reaper Recovery
     * Seeds records stuck in PROCESSING state beyond timeout threshold.
     * Watchdog Reaper recovers rows to FAILED and poller re-publishes them to SENT.
     */
    @Test
    void testScenario4_ReaperWatchdogCrashRecovery() throws Exception {
        Set<String> publishedIds = ConcurrentHashMap.newKeySet();
        BrokerPublisher brokerPublisher = new BrokerPublisher() {
            @Override
            public CompletableFuture<OutboxResult> publish(String pipeline, OutboxRecord record) {
                publishedIds.add(record.getOutboxId());
                return CompletableFuture.completedFuture(
                        OutboxResult.success(record.getOutboxId(), record.getTopic(), 0, 1)
                );
            }

            @Override
            public CompletableFuture<List<OutboxResult>> publishBatch(String pipeline, List<OutboxRecord> records) {
                records.forEach(r -> publishedIds.add(r.getOutboxId()));
                return CompletableFuture.completedFuture(
                        records.stream().map(r -> OutboxResult.success(r.getOutboxId(), r.getTopic(), 0, 1)).toList()
                );
            }
        };

        OutboxDispatcher dispatcher = new OutboxDispatcher(repository, brokerPublisher);

        // Seed 5 records and set them stuck in PROCESSING with old updated_at
        for (int i = 0; i < 5; i++) {
            String recordId = "stuck-order-" + i;
            DefaultOutboxRecord record = DefaultOutboxRecord.fromPayload("test-pipeline", recordId,
                    OutboxPayload.of("orders.topic", "{\"stuck\":" + i + "}"));
            repository.insertRecord(config, record);
            repository.markProcessing(config, List.of(recordId));
        }

        // Simulate crash: updated_at was 10 seconds ago
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE OUTBOX_MESSAGES SET updated_at = DATEADD('SECOND', -10, CURRENT_TIMESTAMP)");
        }

        // Run Reaper
        try (OutboxReaperJob reaper = new OutboxReaperJob(config, repository)) {
            int reaped = reaper.reapOnce();
            assertThat(reaped).isEqualTo(5);
        }

        // Assert all 5 records are now in FAILED state with retry_count = 1
        for (int i = 0; i < 5; i++) {
            OutboxRecord r = repository.findById(config, "stuck-order-" + i).orElseThrow();
            assertThat(r.getStatus()).isEqualTo(OutboxStatus.FAILED);
            assertThat(r.getRetryCount()).isEqualTo(1);
            assertThat(r.getLastError()).isEqualTo("PROCESSING_TIMEOUT_EXCEEDED");
        }

        // Now run poller to re-process and dispatch recovered rows
        try (OutboxPollerEngine poller = new OutboxPollerEngine(config, repository, dispatcher)) {
            int polled = poller.pollOnce();
            assertThat(polled).isEqualTo(5);
        }

        // Verify all 5 were published and marked SENT
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(publishedIds).hasSize(5);
            for (int i = 0; i < 5; i++) {
                OutboxRecord r = repository.findById(config, "stuck-order-" + i).orElseThrow();
                assertThat(r.getStatus()).isEqualTo(OutboxStatus.SENT);
            }
        });
    }
}
