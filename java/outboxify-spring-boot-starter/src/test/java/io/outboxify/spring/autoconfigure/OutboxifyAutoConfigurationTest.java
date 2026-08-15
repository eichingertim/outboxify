/*
 * Copyright 2026 Outboxify Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.outboxify.spring.autoconfigure;

import io.outboxify.core.engine.OutboxDispatcher;
import io.outboxify.core.engine.OutboxHook;
import io.outboxify.core.engine.OutboxPublisher;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.BrokerPublisher;
import io.outboxify.core.spi.DialectType;
import io.outboxify.core.spi.OutboxRepository;
import io.outboxify.dialects.DialectRegistry;
import io.outboxify.spring.lifecycle.OutboxifyLifecycleManager;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxifyAutoConfigurationTest {

    @Configuration
    static class TestDataSourceConfig {
        @Bean
        public DataSource dataSource() {
            org.h2.jdbcx.JdbcDataSource ds = new org.h2.jdbcx.JdbcDataSource();
            ds.setURL("jdbc:h2:mem:autoconfigure_test;DB_CLOSE_DELAY=-1");
            ds.setUser("sa");
            ds.setPassword("");
            return ds;
        }
    }

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestDataSourceConfig.class)
            .withConfiguration(AutoConfigurations.of(OutboxifyAutoConfiguration.class));

    @Test
    void testDefaultAutoConfigurationLoads() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(DialectRegistry.class);
            assertThat(context).hasSingleBean(OutboxRepository.class);
            assertThat(context).hasSingleBean(BrokerPublisher.class);
            assertThat(context).hasSingleBean(OutboxDispatcher.class);
            assertThat(context).hasSingleBean(OutboxHook.class);
            assertThat(context).hasSingleBean(OutboxPublisher.class);
            assertThat(context).hasSingleBean(OutboxifyLifecycleManager.class);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCustomMultiPipelinePropertyBinding() {
        contextRunner
                .withPropertyValues(
                        "outboxify.defaults.batch-size=150",
                        "outboxify.defaults.poll-interval-ms=2000",
                        "outboxify.pipelines.orders.enabled=true",
                        "outboxify.pipelines.orders.table-name=ORDERS",
                        "outboxify.pipelines.orders.dialect=ORACLE",
                        "outboxify.pipelines.orders.batch-size=200",
                        "outboxify.pipelines.orders.columns.id=ORDER_ID",
                        "outboxify.pipelines.orders.columns.topic=KAFKA_TOPIC",
                        "outboxify.pipelines.orders.columns.payload=OUTBOX_PAYLOAD",
                        "outboxify.pipelines.orders.columns.status=OUTBOX_STATUS",
                        "outboxify.pipelines.payments.table-name=OUTBOX_PAYMENTS",
                        "outboxify.pipelines.payments.dialect=POSTGRESQL"
                )
                .run(context -> {
                    Map<String, PipelineConfig> configs = (Map<String, PipelineConfig>) context.getBean("openOutboxPipelineConfigs");
                    assertThat(configs).containsKeys("orders", "payments");

                    PipelineConfig ordersConfig = configs.get("orders");
                    assertThat(ordersConfig.getTableName()).isEqualTo("ORDERS");
                    assertThat(ordersConfig.getDialect()).isEqualTo(DialectType.ORACLE);
                    assertThat(ordersConfig.getBatchSize()).isEqualTo(200);
                    assertThat(ordersConfig.getPollIntervalMs()).isEqualTo(2000L); // inherited from defaults
                    assertThat(ordersConfig.getColumns().getId()).isEqualTo("ORDER_ID");
                    assertThat(ordersConfig.getColumns().getTopic()).isEqualTo("KAFKA_TOPIC");
                    assertThat(ordersConfig.getColumns().getStatus()).isEqualTo("OUTBOX_STATUS");

                    PipelineConfig paymentsConfig = configs.get("payments");
                    assertThat(paymentsConfig.getTableName()).isEqualTo("OUTBOX_PAYMENTS");
                    assertThat(paymentsConfig.getDialect()).isEqualTo(DialectType.POSTGRESQL);
                    assertThat(paymentsConfig.getBatchSize()).isEqualTo(150); // inherited from defaults
                });
    }

    @Test
    void testDisableOutboxifyViaProperty() {
        contextRunner
                .withPropertyValues("outboxify.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(OutboxifyLifecycleManager.class);
                    assertThat(context).doesNotHaveBean(OutboxPublisher.class);
                });
    }
}
