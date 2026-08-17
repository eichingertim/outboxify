package io.outboxify.example.model;

import java.math.BigDecimal;

public record OrderRequest(
        String customerId,
        String item,
        BigDecimal amount
) {}
