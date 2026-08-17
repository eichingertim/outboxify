package io.outboxify.example;

import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxResult;
import io.outboxify.core.spi.BrokerPublisher;
import io.outboxify.example.service.BrokerMessageTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@SpringBootApplication
public class OrderServiceApplication {

    private static final Logger log = LoggerFactory.getLogger(OrderServiceApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
        log.info("""
            ================================================================
               🚀 Outboxify Java Spring Boot Order Service Started!
               
               Endpoints:
                 POST /api/orders             - Create order + outbox event
                 POST /api/orders/simulate-failure - Test rollback safety
                 GET  /api/orders             - List orders
                 GET  /api/outbox             - List outbox table rows
                 GET  /api/broker/messages    - List published broker messages
                 
               H2 Console: http://localhost:8080/h2-console
            ================================================================
            """);
    }

    /**
     * In-memory broker publisher logger fallback for easy zero-setup local demonstration.
     */
    @Bean
    public BrokerPublisher brokerPublisher(BrokerMessageTracker tracker) {
        return new BrokerPublisher() {
            @Override
            public CompletableFuture<OutboxResult> publish(String pipelineName, OutboxRecord record) {
                log.info("🔥 [KAFKA BROKER] Delivered message to topic '{}' (Key: '{}', ID: '{}'): {}",
                        record.getTopic(), record.getPartitionKey(), record.getOutboxId(), record.getPayload());
                tracker.recordDelivery(record);
                return CompletableFuture.completedFuture(OutboxResult.success(record.getOutboxId(), record.getTopic(), 0, 0L));
            }

            @Override
            public CompletableFuture<List<OutboxResult>> publishBatch(String pipelineName, List<OutboxRecord> records) {
                List<OutboxResult> results = records.stream()
                        .map(r -> {
                            tracker.recordDelivery(r);
                            return OutboxResult.success(r.getOutboxId(), r.getTopic(), 0, 0L);
                        })
                        .toList();
                return CompletableFuture.completedFuture(results);
            }

            @Override
            public void close() {
                tracker.clear();
            }
        };
    }
}
