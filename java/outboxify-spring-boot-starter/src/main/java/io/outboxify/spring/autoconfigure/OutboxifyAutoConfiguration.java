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
import io.outboxify.core.model.BrokerConfig;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.BrokerPublisher;
import io.outboxify.core.spi.OutboxRepository;
import io.outboxify.dialects.DialectRegistry;
import io.outboxify.dialects.DynamicSqlRepository;
import io.outboxify.kafka.KafkaBrokerPublisher;
import io.outboxify.spring.config.OutboxifyProperties;
import io.outboxify.spring.datasource.SpringConnectionProvider;
import io.outboxify.spring.hook.TransactionalOutboxHook;
import io.outboxify.spring.lifecycle.OutboxifyLifecycleManager;
import io.outboxify.spring.publisher.SpringOutboxPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Spring Boot AutoConfiguration for Outboxify.
 * Dynamically wires multi-pipeline schedulers, repositories, Kafka publishers, and transaction hooks.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration",
        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration"
})
@ConditionalOnClass({OutboxPublisher.class, DataSource.class})
@ConditionalOnProperty(prefix = "outboxify", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(OutboxifyProperties.class)
public class OutboxifyAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(OutboxifyAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public DialectRegistry openOutboxDialectRegistry() {
        return new DialectRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    public OutboxRepository openOutboxRepository(DataSource dataSource, DialectRegistry dialectRegistry) {
        return new DynamicSqlRepository(dataSource, new SpringConnectionProvider(dataSource), dialectRegistry);
    }

    @Bean
    @ConditionalOnMissingBean(name = "openOutboxPipelineConfigs")
    public Map<String, PipelineConfig> openOutboxPipelineConfigs(OutboxifyProperties properties) {
        Map<String, PipelineConfig> map = new HashMap<>();

        if (properties.getPipelines() == null || properties.getPipelines().isEmpty()) {
            PipelineConfig defaultConfig = properties.toPipelineConfig("default", null);
            map.put("default", defaultConfig);
        } else {
            for (Map.Entry<String, OutboxifyProperties.PipelineConfigProps> entry : properties.getPipelines().entrySet()) {
                PipelineConfig config = properties.toPipelineConfig(entry.getKey(), entry.getValue());
                map.put(entry.getKey(), config);
            }
        }

        return Collections.unmodifiableMap(map);
    }

    @Bean
    @ConditionalOnMissingBean(BrokerPublisher.class)
    public BrokerPublisher openOutboxBrokerPublisher(Map<String, PipelineConfig> openOutboxPipelineConfigs) {
        BrokerConfig defaultBrokerConfig = openOutboxPipelineConfigs.values().stream()
                .map(PipelineConfig::getBroker)
                .findFirst()
                .orElse(BrokerConfig.defaultConfig());

        KafkaBrokerPublisher publisher = new KafkaBrokerPublisher(defaultBrokerConfig);

        for (PipelineConfig config : openOutboxPipelineConfigs.values()) {
            publisher.registerPipelineBroker(config.getName(), config.getBroker());
        }

        return publisher;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({OutboxRepository.class, BrokerPublisher.class})
    public OutboxDispatcher openOutboxDispatcher(OutboxRepository openOutboxRepository, BrokerPublisher openOutboxBrokerPublisher) {
        return new OutboxDispatcher(openOutboxRepository, openOutboxBrokerPublisher);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(OutboxDispatcher.class)
    public OutboxHook openOutboxHook(OutboxDispatcher openOutboxDispatcher, Map<String, PipelineConfig> openOutboxPipelineConfigs) {
        return new TransactionalOutboxHook(openOutboxDispatcher, openOutboxPipelineConfigs::get);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({OutboxRepository.class, OutboxHook.class, OutboxDispatcher.class})
    public OutboxPublisher openOutboxPublisher(OutboxRepository openOutboxRepository,
                                              OutboxHook openOutboxHook,
                                              OutboxDispatcher openOutboxDispatcher,
                                              Map<String, PipelineConfig> openOutboxPipelineConfigs) {
        return new SpringOutboxPublisher(openOutboxRepository, openOutboxHook, openOutboxDispatcher, openOutboxPipelineConfigs::get);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({OutboxRepository.class, OutboxDispatcher.class, BrokerPublisher.class, OutboxHook.class})
    public OutboxifyLifecycleManager openOutboxLifecycleManager(Map<String, PipelineConfig> openOutboxPipelineConfigs,
                                                                OutboxRepository openOutboxRepository,
                                                                OutboxDispatcher openOutboxDispatcher,
                                                                BrokerPublisher openOutboxBrokerPublisher,
                                                                OutboxHook openOutboxHook) {
        return new OutboxifyLifecycleManager(openOutboxPipelineConfigs, openOutboxRepository, openOutboxDispatcher, openOutboxBrokerPublisher, openOutboxHook);
    }
}
