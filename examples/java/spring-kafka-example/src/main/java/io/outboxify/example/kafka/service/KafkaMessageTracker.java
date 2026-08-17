package io.outboxify.example.kafka.service;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory tracker of Kafka messages dispatched through Spring's KafkaTemplate.
 */
@Component
public class KafkaMessageTracker {

    private final List<DispatchedMessage> messages = new CopyOnWriteArrayList<>();

    public void record(String templateName, String topic, String key, String payload, Map<String, String> headers) {
        messages.add(new DispatchedMessage(templateName, topic, key, payload, headers, System.currentTimeMillis()));
    }

    public List<DispatchedMessage> getMessages() {
        return Collections.unmodifiableList(new ArrayList<>(messages));
    }

    public void clear() {
        messages.clear();
    }

    public record DispatchedMessage(
            String templateName,
            String topic,
            String key,
            String payload,
            Map<String, String> headers,
            long timestamp
    ) {}
}
