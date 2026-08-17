package io.outboxify.example.controller;

import io.outboxify.core.model.OutboxRecord;
import io.outboxify.example.model.OrderRequest;
import io.outboxify.example.model.OrderResponse;
import io.outboxify.example.service.BrokerMessageTracker;
import io.outboxify.example.service.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class OrderController {

    private final OrderService orderService;
    private final BrokerMessageTracker brokerMessageTracker;

    public OrderController(OrderService orderService, BrokerMessageTracker brokerMessageTracker) {
        this.orderService = orderService;
        this.brokerMessageTracker = brokerMessageTracker;
    }

    @PostMapping("/orders")
    public ResponseEntity<OrderResponse> createOrder(@RequestBody OrderRequest request) {
        OrderResponse response = orderService.createOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PostMapping("/orders/simulate-failure")
    public ResponseEntity<?> createOrderWithFailure(@RequestBody OrderRequest request) {
        try {
            orderService.createOrderWithSimulatedFailure(request);
            return ResponseEntity.ok("Unexpected success");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ROLLED_BACK",
                    "error", ex.getMessage(),
                    "message", "Transaction was rolled back. Outboxify guarantees NO orphaned event was published to Kafka!"
            ));
        }
    }

    @GetMapping("/orders")
    public ResponseEntity<List<Map<String, Object>>> getOrders() {
        return ResponseEntity.ok(orderService.getAllOrders());
    }

    @GetMapping("/outbox")
    public ResponseEntity<List<Map<String, Object>>> getOutbox() {
        return ResponseEntity.ok(orderService.getOutboxRecords());
    }

    @GetMapping("/broker/messages")
    public ResponseEntity<List<OutboxRecord>> getBrokerMessages() {
        return ResponseEntity.ok(brokerMessageTracker.getDeliveredMessages());
    }
}
