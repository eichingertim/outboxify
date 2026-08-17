package io.outboxify.example.kafka.controller;

import io.outboxify.example.kafka.model.PaymentRequest;
import io.outboxify.example.kafka.model.PaymentResponse;
import io.outboxify.example.kafka.service.KafkaMessageTracker;
import io.outboxify.example.kafka.service.PaymentService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class PaymentController {

    private final PaymentService paymentService;
    private final KafkaMessageTracker messageTracker;

    public PaymentController(PaymentService paymentService, KafkaMessageTracker messageTracker) {
        this.paymentService = paymentService;
        this.messageTracker = messageTracker;
    }

    @PostMapping("/payments")
    public ResponseEntity<PaymentResponse> processPayment(@RequestBody PaymentRequest request) {
        PaymentResponse response = paymentService.processPayment(request, false);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PostMapping("/payments/simulate-failure")
    public ResponseEntity<Map<String, String>> simulateFailure(@RequestBody PaymentRequest request) {
        try {
            paymentService.processPayment(request, true);
            return ResponseEntity.ok(Map.of("message", "Payment processed (unexpected)"));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ROLLED_BACK",
                    "error", e.getMessage()
            ));
        }
    }

    @GetMapping("/payments")
    public ResponseEntity<List<Map<String, Object>>> listPayments() {
        return ResponseEntity.ok(paymentService.listPayments());
    }

    @GetMapping("/outbox")
    public ResponseEntity<List<Map<String, Object>>> listStandardOutbox() {
        return ResponseEntity.ok(paymentService.listOutboxRecords("payments"));
    }

    @GetMapping("/outbox/high-priority")
    public ResponseEntity<List<Map<String, Object>>> listHighPriorityOutbox() {
        return ResponseEntity.ok(paymentService.listOutboxRecords("high_priority_payments"));
    }

    @GetMapping("/broker/messages")
    public ResponseEntity<List<KafkaMessageTracker.DispatchedMessage>> listBrokerMessages() {
        return ResponseEntity.ok(messageTracker.getMessages());
    }

    @PostMapping("/broker/messages/clear")
    public ResponseEntity<Map<String, String>> clearMessages() {
        messageTracker.clear();
        return ResponseEntity.ok(Map.of("status", "cleared"));
    }
}
