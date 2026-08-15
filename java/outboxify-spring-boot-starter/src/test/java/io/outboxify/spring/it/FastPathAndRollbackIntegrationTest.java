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

import io.outboxify.core.engine.OutboxPublisher;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxResult;
import io.outboxify.core.model.OutboxStatus;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.BrokerPublisher;
import io.outboxify.core.spi.OutboxRepository;
import io.outboxify.spring.autoconfigure.OutboxifyAutoConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = FastPathAndRollbackIntegrationTest.TestApp.class, properties = {
        "spring.datasource.url=jdbc:h2:mem:fastpath_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "outboxify.pipelines.orders.table-name=ORDERS_OUTBOX",
        "outboxify.pipelines.orders.dialect=H2",
        "outboxify.pipelines.orders.immediate-send.enabled=true"
})
class FastPathAndRollbackIntegrationTest {

    @Service
    public static class OrderService {
        private final OutboxPublisher outboxPublisher;

        public OrderService(OutboxPublisher outboxPublisher) {
            this.outboxPublisher = outboxPublisher;
        }

        @Transactional
        public String createOrderSuccessfully(String orderId, String payload) {
            return outboxPublisher.publish("orders", OutboxPayload.of("orders.v1", orderId, payload)).join().getRecordId();
        }

        @Transactional
        public String createOrderWithRollback(String orderId, String payload) {
            outboxPublisher.publish("orders", OutboxPayload.of("orders.v1", orderId, payload));
            throw new IllegalStateException("Simulated transactional rollback failure");
        }
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        @Primary
        public BrokerPublisher mockBrokerPublisher() {
            return new MockBrokerPublisher();
        }
    }

    @SpringBootApplication
    @Import({
            OutboxifyAutoConfiguration.class,
            OrderService.class,
            TestConfig.class
    })
    static class TestApp {
    }

    static class MockBrokerPublisher implements BrokerPublisher {
        final List<OutboxRecord> publishedRecords = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<OutboxResult> publish(String pipeline, OutboxRecord record) {
            publishedRecords.add(record);
            return CompletableFuture.completedFuture(
                    OutboxResult.success(record.getOutboxId(), record.getTopic(), 0, publishedRecords.size())
            );
        }

        @Override
        public CompletableFuture<List<OutboxResult>> publishBatch(String pipeline, List<OutboxRecord> records) {
            publishedRecords.addAll(records);
            List<OutboxResult> results = records.stream()
                    .map(r -> OutboxResult.success(r.getOutboxId(), r.getTopic(), 0, publishedRecords.size()))
                    .toList();
            return CompletableFuture.completedFuture(results);
        }
    }

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private Map<String, PipelineConfig> openOutboxPipelineConfigs;

    @Autowired
    private BrokerPublisher brokerPublisher;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private MockBrokerPublisher mockBroker;

    @BeforeEach
    void setUp() {
        mockBroker = (MockBrokerPublisher) brokerPublisher;
        mockBroker.publishedRecords.clear();

        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS ORDERS_OUTBOX (" +
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

        jdbcTemplate.execute("DELETE FROM ORDERS_OUTBOX");
    }

    @Test
    void testScenario1_FastPathCommitHookDispatchesMessage() {
        String recordId = orderService.createOrderSuccessfully("order-101", "{\"orderId\":\"101\",\"amount\":99.99}");
        assertThat(recordId).isNotNull();

        // Verify message received in broker
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(mockBroker.publishedRecords).hasSize(1);
            assertThat(mockBroker.publishedRecords.get(0).getOutboxId()).isEqualTo(recordId);
            assertThat(mockBroker.publishedRecords.get(0).getTopic()).isEqualTo("orders.v1");
            assertThat(mockBroker.publishedRecords.get(0).getPartitionKey()).isEqualTo("order-101");
        });

        // Verify DB row status is updated to SENT
        PipelineConfig config = openOutboxPipelineConfigs.get("orders");
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<OutboxRecord> record = outboxRepository.findById(config, recordId);
            assertThat(record).isPresent();
            assertThat(record.get().getStatus()).isEqualTo(OutboxStatus.SENT);
            assertThat(record.get().getProcessedAt()).isNotNull();
        });
    }

    @Test
    void testScenario2_TransactionRollbackSafetyProducesZeroMessages() {
        assertThatThrownBy(() -> orderService.createOrderWithRollback("order-102", "{\"orderId\":\"102\"}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Simulated transactional rollback failure");

        // Verify 0 messages in broker
        assertThat(mockBroker.publishedRecords).isEmpty();

        // Verify 0 rows in database
        Integer rowCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM ORDERS_OUTBOX", Integer.class);
        assertThat(rowCount).isEqualTo(0);
    }
}
