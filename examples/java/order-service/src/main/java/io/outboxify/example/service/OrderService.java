package io.outboxify.example.service;

import io.outboxify.core.engine.OutboxPublisher;
import io.outboxify.core.model.OutboxPayload;
import io.outboxify.core.model.OutboxResult;
import io.outboxify.example.model.OrderRequest;
import io.outboxify.example.model.OrderResponse;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final JdbcTemplate jdbcTemplate;
    private final OutboxPublisher outboxPublisher;

    public OrderService(JdbcTemplate jdbcTemplate, OutboxPublisher outboxPublisher) {
        this.jdbcTemplate = jdbcTemplate;
        this.outboxPublisher = outboxPublisher;
    }

    @PostConstruct
    public void initDatabase() {
        log.info("Initializing H2 database tables: ORDERS and ORDERS_OUTBOX...");
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS ORDERS (
                id VARCHAR(64) PRIMARY KEY,
                customer_id VARCHAR(64) NOT NULL,
                item VARCHAR(255) NOT NULL,
                amount DECIMAL(10, 2) NOT NULL,
                status VARCHAR(32) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS ORDERS_OUTBOX (
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
        log.info("H2 database schema initialized successfully.");
    }

    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        String orderId = "ord-" + UUID.randomUUID().toString().substring(0, 8);
        Instant now = Instant.now();

        // 1. Write domain business state to ORDERS table
        jdbcTemplate.update("""
            INSERT INTO ORDERS (id, customer_id, item, amount, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, orderId, request.customerId(), request.item(), request.amount(), "CONFIRMED", Timestamp.from(now));
        log.info("[DB] Saved order {} for customer {}", orderId, request.customerId());

        // 2. Stage event in transactional outbox using Outboxify
        String eventPayload = """
            {"orderId":"%s","customerId":"%s","item":"%s","amount":%s,"timestamp":"%s"}
        """.formatted(orderId, request.customerId(), request.item(), request.amount(), now);

        OutboxResult outboxResult = outboxPublisher.publish(
                "orders",
                OutboxPayload.of("orders.events", request.customerId(), eventPayload)
        ).join();

        log.info("[Outboxify] Staged outbox record {} for order {}", outboxResult.getRecordId(), orderId);

        return new OrderResponse(
                orderId,
                request.customerId(),
                request.item(),
                request.amount(),
                "CONFIRMED",
                outboxResult.getRecordId(),
                now
        );
    }

    @Transactional
    public OrderResponse createOrderWithSimulatedFailure(OrderRequest request) {
        String orderId = "ord-fail-" + UUID.randomUUID().toString().substring(0, 8);
        Instant now = Instant.now();

        // 1. Insert domain state
        jdbcTemplate.update("""
            INSERT INTO ORDERS (id, customer_id, item, amount, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, orderId, request.customerId(), request.item(), request.amount(), "PENDING", Timestamp.from(now));

        // 2. Stage Outbox event
        String eventPayload = """
            {"orderId":"%s","customerId":"%s","item":"%s","amount":%s,"timestamp":"%s"}
        """.formatted(orderId, request.customerId(), request.item(), request.amount(), now);

        outboxPublisher.publish(
                "orders",
                OutboxPayload.of("orders.events", request.customerId(), eventPayload)
        );

        log.warn("[Simulation] Intentionally throwing exception to trigger transaction rollback!");
        throw new IllegalStateException("Simulated Payment Gateway Error: Order " + orderId + " failed!");
    }

    public List<Map<String, Object>> getAllOrders() {
        return jdbcTemplate.queryForList("SELECT * FROM ORDERS ORDER BY created_at DESC");
    }

    public List<Map<String, Object>> getOutboxRecords() {
        return jdbcTemplate.queryForList("SELECT * FROM ORDERS_OUTBOX ORDER BY created_at DESC");
    }
}
