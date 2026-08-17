package io.outboxify.example.model;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderResponse(
        String orderId,
        String customerId,
        String item,
        BigDecimal amount,
        String status,
        String outboxRecordId,
        Instant createdAt
) {}
