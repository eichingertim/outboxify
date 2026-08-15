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

import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxResult;
import io.outboxify.core.spi.BrokerPublisher;
import io.outboxify.spring.autoconfigure.OutboxifyAutoConfiguration;
import io.outboxify.spring.jpa.OutboxEntityListener;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = JpaEntityListenerIT.TestConfig.class, properties = {
        "spring.datasource.url=jdbc:h2:mem:jpa_entity_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.show-sql=false",
        "outboxify.pipelines.orders.table-name=ORDERS",
        "outboxify.pipelines.orders.dialect=H2",
        "outboxify.pipelines.orders.columns.id=order_id",
        "outboxify.pipelines.orders.columns.topic=kafka_topic",
        "outboxify.pipelines.orders.columns.partition-key=customer_id",
        "outboxify.pipelines.orders.columns.payload=outbox_payload",
        "outboxify.pipelines.orders.columns.headers=outbox_headers",
        "outboxify.pipelines.orders.columns.status=outbox_status",
        "outboxify.pipelines.orders.columns.retry-count=retry_count",
        "outboxify.pipelines.orders.columns.last-error=last_error",
        "outboxify.pipelines.orders.columns.created-at=created_at",
        "outboxify.pipelines.orders.columns.updated-at=updated_at",
        "outboxify.pipelines.orders.columns.processed-at=processed_at"
})
class JpaEntityListenerIT {

    @Entity
    @Table(name = "ORDERS")
    @EntityListeners(OutboxEntityListener.class)
    public static class OrderEntity implements OutboxRecord {

        @Id
        @Column(name = "order_id")
        private String orderId;

        @Column(name = "customer_id")
        private String customerId;

        @Column(name = "kafka_topic")
        private String kafkaTopic;

        @Column(name = "outbox_payload", length = 4000)
        private String outboxPayload;

        @Column(name = "outbox_headers")
        private String outboxHeaders;

        @Column(name = "outbox_status")
        private String outboxStatus = "NEW";

        @Column(name = "retry_count")
        private int retryCount = 0;

        @Column(name = "last_error")
        private String lastError;

        @Column(name = "created_at")
        private Instant createdAt = Instant.now();

        @Column(name = "updated_at")
        private Instant updatedAt = Instant.now();

        @Column(name = "processed_at")
        private Instant processedAt;

        public OrderEntity() {
        }

        public OrderEntity(String orderId, String customerId, String kafkaTopic, String outboxPayload) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.kafkaTopic = kafkaTopic;
            this.outboxPayload = outboxPayload;
            this.outboxStatus = "NEW";
            this.createdAt = Instant.now();
            this.updatedAt = Instant.now();
        }

        @Override
        public String getOutboxPipeline() {
            return "orders";
        }

        @Override
        public String getOutboxId() {
            return orderId;
        }

        @Override
        public String getTopic() {
            return kafkaTopic;
        }

        @Override
        public String getPartitionKey() {
            return customerId;
        }

        @Override
        public String getPayload() {
            return outboxPayload;
        }

        @Override
        public Map<String, String> getHeaders() {
            return Map.of("eventType", "OrderCreated");
        }

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public String getKafkaTopic() { return kafkaTopic; }
        public void setKafkaTopic(String kafkaTopic) { this.kafkaTopic = kafkaTopic; }
        public String getOutboxPayload() { return outboxPayload; }
        public void setOutboxPayload(String outboxPayload) { this.outboxPayload = outboxPayload; }
        public String getOutboxStatus() { return outboxStatus; }
        public void setOutboxStatus(String outboxStatus) { this.outboxStatus = outboxStatus; }
        public Instant getCreatedAt() { return createdAt; }
        public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
        public Instant getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
        public Instant getProcessedAt() { return processedAt; }
        public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }
    }

    @Repository
    public interface OrderRepository extends JpaRepository<OrderEntity, String> {
    }

    @Service
    public static class OrderDomainService {
        private final OrderRepository orderRepository;

        public OrderDomainService(OrderRepository orderRepository) {
            this.orderRepository = orderRepository;
        }

        @Transactional
        public OrderEntity saveOrder(OrderEntity order) {
            return orderRepository.save(order);
        }
    }

    @SpringBootApplication
    @EnableJpaRepositories(basePackageClasses = JpaEntityListenerIT.class, considerNestedRepositories = true)
    @Import({OutboxifyAutoConfiguration.class, OrderDomainService.class})
    static class TestConfig {
        @Bean
        @Primary
        public BrokerPublisher mockBrokerPublisher() {
            return new MockBrokerPublisher();
        }
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
    private OrderDomainService orderDomainService;

    @Autowired
    private BrokerPublisher brokerPublisher;

    private MockBrokerPublisher mockBroker;

    @BeforeEach
    void setUp() {
        mockBroker = (MockBrokerPublisher) brokerPublisher;
        mockBroker.publishedRecords.clear();
    }

    @Test
    void testEmbeddedDomainEntityJpaInterception() {
        OrderEntity order = new OrderEntity(
                "ord-999",
                "cust-456",
                "orders.domain.events",
                "{\"orderId\":\"ord-999\",\"amount\":250.00}"
        );

        orderDomainService.saveOrder(order);

        // Verify entity listener triggered fast-path dispatch upon transaction commit
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(mockBroker.publishedRecords).hasSize(1);
            OutboxRecord dispatched = mockBroker.publishedRecords.get(0);
            assertThat(dispatched.getOutboxId()).isEqualTo("ord-999");
            assertThat(dispatched.getTopic()).isEqualTo("orders.domain.events");
            assertThat(dispatched.getPartitionKey()).isEqualTo("cust-456");
            assertThat(dispatched.getHeaders()).containsEntry("eventType", "OrderCreated");
        });
    }
}
