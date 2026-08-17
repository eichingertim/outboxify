package io.outboxify.example.kafka;

import io.outboxify.core.spi.BrokerPublisher;
import io.outboxify.example.kafka.model.PaymentRequest;
import io.outboxify.example.kafka.model.PaymentResponse;
import io.outboxify.example.kafka.service.KafkaMessageTracker;
import io.outboxify.example.kafka.service.PaymentService;
import io.outboxify.spring.kafka.SpringKafkaBrokerPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class SpringKafkaExampleApplicationTests {

    @Autowired
    private BrokerPublisher brokerPublisher;

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private KafkaMessageTracker messageTracker;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        messageTracker.clear();
        jdbcTemplate.execute("DELETE FROM PAYMENTS");
        jdbcTemplate.execute("DELETE FROM PAYMENTS_OUTBOX");
        jdbcTemplate.execute("DELETE FROM HIGH_PRIORITY_OUTBOX");
    }

    @Test
    void testContextWiresSpringKafkaBrokerPublisher() {
        assertThat(brokerPublisher)
                .isNotNull()
                .isInstanceOf(SpringKafkaBrokerPublisher.class);
    }

    @Test
    void testStandardPaymentDispatchesViaPrimaryKafkaTemplate() {
        PaymentRequest request = new PaymentRequest("cust-100", new BigDecimal("120.00"), "USD", false);
        PaymentResponse response = paymentService.processPayment(request, false);

        assertThat(response.status()).isEqualTo("SETTLED");
        assertThat(response.pipeline()).isEqualTo("payments");

        // Verify DB state
        List<String> paymentIds = jdbcTemplate.queryForList("SELECT id FROM PAYMENTS WHERE customer_id = 'cust-100'", String.class);
        assertThat(paymentIds).hasSize(1);

        // Verify Kafka dispatch
        List<KafkaMessageTracker.DispatchedMessage> messages = messageTracker.getMessages();
        assertThat(messages).hasSize(1);

        KafkaMessageTracker.DispatchedMessage msg = messages.get(0);
        assertThat(msg.templateName()).isEqualTo("primary-kafka-template");
        assertThat(msg.topic()).isEqualTo("payments.standard.v1");
        assertThat(msg.key()).isEqualTo("cust-100");
        assertThat(msg.payload()).contains("cust-100");
        assertThat(msg.headers()).containsEntry("pipeline", "payments");
    }

    @Test
    void testHighPriorityPaymentDispatchesViaHighPriorityKafkaTemplate() {
        PaymentRequest request = new PaymentRequest("cust-vip-1", new BigDecimal("9999.00"), "EUR", true);
        PaymentResponse response = paymentService.processPayment(request, false);

        assertThat(response.status()).isEqualTo("SETTLED");
        assertThat(response.pipeline()).isEqualTo("high_priority_payments");

        // Verify DB state
        List<String> paymentIds = jdbcTemplate.queryForList("SELECT id FROM PAYMENTS WHERE customer_id = 'cust-vip-1'", String.class);
        assertThat(paymentIds).hasSize(1);

        // Verify Kafka dispatch routes through the high priority template
        List<KafkaMessageTracker.DispatchedMessage> messages = messageTracker.getMessages();
        assertThat(messages).hasSize(1);

        KafkaMessageTracker.DispatchedMessage msg = messages.get(0);
        assertThat(msg.templateName()).isEqualTo("high-priority-kafka-template");
        assertThat(msg.topic()).isEqualTo("payments.vip.v1");
        assertThat(msg.key()).isEqualTo("cust-vip-1");
        assertThat(msg.payload()).contains("cust-vip-1");
        assertThat(msg.headers()).containsEntry("pipeline", "high_priority_payments");
    }

    @Test
    void testRollbackSafetyPreventsKafkaDispatch() {
        PaymentRequest request = new PaymentRequest("cust-err", new BigDecimal("50.00"), "USD", false);

        assertThatThrownBy(() -> paymentService.processPayment(request, true))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated payment gateway timeout");

        // Verify no payments or outbox records in DB
        Integer paymentCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM PAYMENTS WHERE customer_id = 'cust-err'", Integer.class);
        assertThat(paymentCount).isEqualTo(0);

        Integer outboxCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM PAYMENTS_OUTBOX", Integer.class);
        assertThat(outboxCount).isEqualTo(0);

        // Verify 0 messages dispatched to Kafka
        assertThat(messageTracker.getMessages()).isEmpty();
    }
}
