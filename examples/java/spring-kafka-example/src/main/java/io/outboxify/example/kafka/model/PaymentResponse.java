package io.outboxify.example.kafka.model;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentResponse(
        String paymentId,
        String customerId,
        BigDecimal amount,
        String currency,
        String pipeline,
        String status,
        String outboxRecordId,
        Instant createdAt
) {}
