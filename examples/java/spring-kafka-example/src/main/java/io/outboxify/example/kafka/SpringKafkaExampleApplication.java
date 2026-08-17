package io.outboxify.example.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringKafkaExampleApplication {

    private static final Logger log = LoggerFactory.getLogger(SpringKafkaExampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaExampleApplication.class, args);
        log.info("""
            ========================================================================================
               🚀 Outboxify Spring Kafka (KafkaTemplate Injection) Example Started!
               
               Demonstrating:
                 - Zero-redefinition KafkaTemplate auto-wiring
                 - Multi-pipeline template routing via 'kafka-template-ref'
                 - Transactional fast-path dual-write dispatch
                 
               Endpoints:
                 POST /api/payments                  - Process standard payment (primary KafkaTemplate)
                 POST /api/payments                  - Set highPriority=true (highPriorityKafkaTemplate)
                 POST /api/payments/simulate-failure - Test rollback safety
                 GET  /api/payments                  - List settled payments
                 GET  /api/outbox                    - List standard outbox table rows
                 GET  /api/outbox/high-priority      - List high priority outbox table rows
                 GET  /api/broker/messages           - List messages dispatched via Spring KafkaTemplates
                 
               H2 Console: http://localhost:8081/h2-console (JDBC URL: jdbc:h2:mem:payments_db)
            ========================================================================================
            """);
    }
}
