package io.outboxify.example.kafka.model;

import java.math.BigDecimal;

public class PaymentRequest {

    private String customerId;
    private BigDecimal amount;
    private String currency;
    private boolean highPriority;

    public PaymentRequest() {}

    public PaymentRequest(String customerId, BigDecimal amount, String currency, boolean highPriority) {
        this.customerId = customerId;
        this.amount = amount;
        this.currency = currency;
        this.highPriority = highPriority;
    }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public boolean isHighPriority() { return highPriority; }
    public void setHighPriority(boolean highPriority) { this.highPriority = highPriority; }
}
