package io.outboxify.example.service;

import io.outboxify.core.model.OutboxRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class BrokerMessageTracker {

    private final List<OutboxRecord> deliveredMessages = new CopyOnWriteArrayList<>();

    public void recordDelivery(OutboxRecord record) {
        deliveredMessages.add(record);
    }

    public List<OutboxRecord> getDeliveredMessages() {
        return List.copyOf(deliveredMessages);
    }

    public void clear() {
        deliveredMessages.clear();
    }
}
