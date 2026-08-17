package io.outboxify.example.kafka.service;

import io.outboxify.core.engine.OutboxPublisher;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxRecord;
import io.outboxify.example.kafka.model.PaymentRequest;
import io.outboxify.example.kafka.model.PaymentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final JdbcTemplate jdbcTemplate;
    private final OutboxPublisher outboxPublisher;

    public PaymentService(JdbcTemplate jdbcTemplate, OutboxPublisher outboxPublisher) {
        this.jdbcTemplate = jdbcTemplate;
        this.outboxPublisher = outboxPublisher;
    }

    @jakarta.annotation.PostConstruct
    public void initDatabase() {
        log.info("Initializing H2 database tables: PAYMENTS, PAYMENTS_OUTBOX, HIGH_PRIORITY_OUTBOX...");
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS PAYMENTS (
                id VARCHAR(64) PRIMARY KEY,
                customer_id VARCHAR(64) NOT NULL,
                amount DECIMAL(10, 2) NOT NULL,
                currency VARCHAR(16) NOT NULL,
                high_priority BOOLEAN DEFAULT FALSE,
                status VARCHAR(32) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS PAYMENTS_OUTBOX (
                id VARCHAR(64) PRIMARY KEY,
                topic VARCHAR(255) NOT NULL,
                partition_key VARCHAR(255),
                payload TEXT NOT NULL,
                headers TEXT,
                status VARCHAR(32) NOT NULL,
                retry_count INT DEFAULT 0,
                last_error TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP WITH TIME ZONE
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS HIGH_PRIORITY_OUTBOX (
                id VARCHAR(64) PRIMARY KEY,
                topic VARCHAR(255) NOT NULL,
                partition_key VARCHAR(255),
                payload TEXT NOT NULL,
                headers TEXT,
                status VARCHAR(32) NOT NULL,
                retry_count INT DEFAULT 0,
                last_error TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP WITH TIME ZONE
            )
        """);
        log.info("H2 database tables initialized.");
    }

    @Transactional
    public PaymentResponse processPayment(PaymentRequest request, boolean simulateFailure) {
        String paymentId = "pay-" + UUID.randomUUID().toString().substring(0, 8);
        Instant now = Instant.now();
        String pipeline = request.isHighPriority() ? "high_priority_payments" : "payments";
        String topic = request.isHighPriority() ? "payments.vip.v1" : "payments.standard.v1";

        log.info("Processing payment '{}' for customer '{}' (Amount: {} {}, Pipeline: {})",
                paymentId, request.getCustomerId(), request.getAmount(), request.getCurrency(), pipeline);

        // 1. Insert domain entity into PAYMENTS table
        jdbcTemplate.update(
                "INSERT INTO PAYMENTS (id, customer_id, amount, currency, high_priority, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                paymentId, request.getCustomerId(), request.getAmount(), request.getCurrency(), request.isHighPriority(), "SETTLED", now
        );

        // 2. Stage outbox record in the same transaction
        String payloadJson = String.format(
                "{\"paymentId\":\"%s\",\"customerId\":\"%s\",\"amount\":%s,\"currency\":\"%s\",\"priority\":\"%s\"}",
                paymentId, request.getCustomerId(), request.getAmount(), request.getCurrency(), request.isHighPriority() ? "HIGH" : "STANDARD"
        );

        io.outboxify.core.model.OutboxResult outboxResult = outboxPublisher.publish(pipeline, OutboxPayload.builder()
                .topic(topic)
                .partitionKey(request.getCustomerId())
                .payload(payloadJson)
                .headers(Map.of(
                        "traceId", UUID.randomUUID().toString(),
                        "eventType", "PaymentSettled",
                        "pipeline", pipeline
                ))
                .build()).join();

        // 3. Simulate rollback if requested
        if (simulateFailure) {
            log.warn("⚠️ Simulating processing exception to trigger transactional rollback for payment '{}'", paymentId);
            throw new RuntimeException("Simulated payment gateway timeout: Rolling back transaction!");
        }

        return new PaymentResponse(
                paymentId,
                request.getCustomerId(),
                request.getAmount(),
                request.getCurrency(),
                pipeline,
                "SETTLED",
                outboxResult.getRecordId(),
                now
        );
    }

    public List<Map<String, Object>> listPayments() {
        return jdbcTemplate.queryForList("SELECT * FROM PAYMENTS ORDER BY created_at DESC");
    }

    public List<Map<String, Object>> listOutboxRecords(String pipeline) {
        String table = "high_priority_payments".equals(pipeline) ? "HIGH_PRIORITY_OUTBOX" : "PAYMENTS_OUTBOX";
        return jdbcTemplate.queryForList("SELECT * FROM " + table + " ORDER BY created_at DESC");
    }
}
