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

package io.outboxify.spring.it.containers;

import io.outboxify.core.engine.OutboxPublisher;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxStatus;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.OutboxRepository;
import io.outboxify.spring.autoconfigure.OutboxifyAutoConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(classes = PostgresKafkaTestcontainersIT.TestApp.class)
class PostgresKafkaTestcontainersIT {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("outbox_db")
            .withUsername("outbox_user")
            .withPassword("outbox_pass");

    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    static {
        postgres.start();
        kafka.start();

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             java.sql.Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS ORDERS_OUTBOX (" +
                    "id VARCHAR(64) PRIMARY KEY, " +
                    "topic VARCHAR(255) NOT NULL, " +
                    "partition_key VARCHAR(255), " +
                    "payload TEXT NOT NULL, " +
                    "headers TEXT, " +
                    "status VARCHAR(32) NOT NULL, " +
                    "retry_count INT DEFAULT 0, " +
                    "last_error TEXT, " +
                    "created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, " +
                    "updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, " +
                    "processed_at TIMESTAMP WITH TIME ZONE)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_orders_outbox_sparse ON ORDERS_OUTBOX (created_at) WHERE status IN ('NEW', 'FAILED')");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize PostgreSQL schema", e);
        }
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");

        registry.add("outboxify.pipelines.orders.table-name", () -> "ORDERS_OUTBOX");
        registry.add("outboxify.pipelines.orders.dialect", () -> "POSTGRESQL");
        registry.add("outboxify.pipelines.orders.batch-size", () -> 100);
        registry.add("outboxify.pipelines.orders.poll-interval-ms", () -> 500);
        registry.add("outboxify.pipelines.orders.immediate-send.enabled", () -> true);
        registry.add("outboxify.pipelines.orders.broker.producer.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("outboxify.pipelines.orders.broker.producer.acks", () -> "all");
        registry.add("outboxify.pipelines.orders.broker.producer.enable-idempotence", () -> true);
    }

    @Service
    public static class OrderService {
        private final OutboxPublisher outboxPublisher;

        public OrderService(OutboxPublisher outboxPublisher) {
            this.outboxPublisher = outboxPublisher;
        }

        @Transactional
        public String createOrder(String orderId, String payload) {
            return outboxPublisher.publish("orders", OutboxPayload.of("orders.e2e.topic", orderId, payload)).join().getRecordId();
        }

        @Transactional
        public void createOrderWithRollback(String orderId, String payload) {
            outboxPublisher.publish("orders", OutboxPayload.of("orders.e2e.topic", orderId, payload));
            throw new RuntimeException("Forced rollback in PostgreSQL transaction");
        }
    }

    @SpringBootApplication
    @Import(OrderService.class)
    static class TestApp {
    }

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private Map<String, PipelineConfig> openOutboxPipelineConfigs;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static KafkaConsumer<String, String> consumer;

    @BeforeAll
    static void initConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("orders.e2e.topic"));
    }

    @AfterAll
    static void closeConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @BeforeEach
    void setupDatabase() {
        jdbcTemplate.execute("DELETE FROM ORDERS_OUTBOX");
        consumer.poll(Duration.ofMillis(100));
    }

    /**
     * Scenario 1: Fast-Path Dispatch with Real PostgreSQL and Real Kafka KRaft
     */
    @Test
    void testE2E_FastPathSendAndVerification() {
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        String recordId = orderService.createOrder("cust-999", "{\"orderId\":\"ord-1001\",\"amount\":199.99}");
        assertThat(recordId).isNotNull();

        // 1. Verify message in Kafka broker
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> r : records) {
                if ("cust-999".equals(r.key())) {
                    matches.add(r);
                }
            }
            assertThat(matches).isNotEmpty();
            assertThat(matches.get(0).value()).isEqualTo("{\"orderId\":\"ord-1001\",\"amount\":199.99}");
        });

        // 2. Verify PostgreSQL row status is updated to SENT
        PipelineConfig config = openOutboxPipelineConfigs.get("orders");
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<OutboxRecord> record = outboxRepository.findById(config, recordId);
            assertThat(record).isPresent();
            assertThat(record.get().getStatus()).isEqualTo(OutboxStatus.SENT);
            assertThat(record.get().getProcessedAt()).isNotNull();
        });
    }

    /**
     * Scenario 2: Rollback Safety on PostgreSQL
     */
    @Test
    void testE2E_RollbackSafetyZeroDualWrite() {
        assertThatThrownBy(() -> orderService.createOrderWithRollback("cust-err", "{\"orderId\":\"failed\"}"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Forced rollback in PostgreSQL transaction");

        // Verify 0 rows in PostgreSQL table
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM ORDERS_OUTBOX WHERE partition_key = 'cust-err'", Integer.class);
        assertThat(count).isEqualTo(0);

        // Verify 0 messages in Kafka
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : records) {
            assertThat(r.key()).isNotEqualTo("cust-err");
        }
    }
}
