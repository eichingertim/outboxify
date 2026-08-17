package io.outboxify.example.kafka.config;

import io.outboxify.example.kafka.service.KafkaMessageTracker;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring Kafka configuration demonstrating custom KafkaTemplate injection and multi-template routing.
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    /**
     * Primary default KafkaTemplate automatically detected by Outboxify for general pipelines.
     */
    @Bean
    @Primary
    public KafkaTemplate<String, String> kafkaTemplate(KafkaMessageTracker tracker) {
        log.info("Configuring primary Spring KafkaTemplate bean");
        KafkaTemplate<String, String> template = new KafkaTemplate<>(createMockProducerFactory());
        template.setProducerListener(new TrackingProducerListener("primary-kafka-template", tracker));
        return template;
    }

    /**
     * Specialized high-priority KafkaTemplate referenced explicitly in outboxify.pipelines.high_priority_payments.broker.kafka-template-ref
     */
    @Bean("highPriorityKafkaTemplate")
    public KafkaTemplate<String, String> highPriorityKafkaTemplate(KafkaMessageTracker tracker) {
        log.info("Configuring specialized 'highPriorityKafkaTemplate' bean for VIP payment pipelines");
        KafkaTemplate<String, String> template = new KafkaTemplate<>(createMockProducerFactory());
        template.setProducerListener(new TrackingProducerListener("high-priority-kafka-template", tracker));
        return template;
    }

    private ProducerFactory<String, String> createMockProducerFactory() {
        MockProducer<String, String> mockProducer = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        return new ProducerFactory<>() {
            @Override
            public Producer<String, String> createProducer() {
                return mockProducer;
            }
        };
    }

    private static class TrackingProducerListener implements ProducerListener<String, String> {
        private final String templateName;
        private final KafkaMessageTracker tracker;

        TrackingProducerListener(String templateName, KafkaMessageTracker tracker) {
            this.templateName = templateName;
            this.tracker = tracker;
        }

        @Override
        public void onSuccess(ProducerRecord<String, String> record, RecordMetadata recordMetadata) {
            Map<String, String> headers = new HashMap<>();
            if (record.headers() != null) {
                for (Header h : record.headers()) {
                    headers.put(h.key(), new String(h.value()));
                }
            }

            log.info("🚀 [KafkaTemplate: {}] Sent message to topic '{}' [Key: '{}']: {}",
                    templateName, record.topic(), record.key(), record.value());

            tracker.record(templateName, record.topic(), record.key(), record.value(), headers);
        }
    }
}
