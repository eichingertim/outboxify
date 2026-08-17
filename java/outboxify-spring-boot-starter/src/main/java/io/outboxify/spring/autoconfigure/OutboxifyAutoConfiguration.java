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
import io.outboxify.spring.kafka.SpringKafkaBrokerPublisher;
import io.outboxify.spring.lifecycle.OutboxifyLifecycleManager;
import io.outboxify.spring.publisher.SpringOutboxPublisher;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Spring Boot AutoConfiguration for Outboxify.
 * Dynamically wires multi-pipeline schedulers, repositories, Kafka publishers, and transaction hooks.
 */
@AutoConfiguration
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

    /**
     * Configuration when Spring Kafka's KafkaTemplate is present on classpath and available as a bean.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(KafkaTemplate.class)
    static class SpringKafkaPublisherConfiguration {

        @Bean
        @ConditionalOnMissingBean(BrokerPublisher.class)
        @ConditionalOnBean(KafkaTemplate.class)
        public BrokerPublisher openOutboxSpringKafkaBrokerPublisher(
                ObjectProvider<KafkaTemplate<?, ?>> kafkaTemplateProvider,
                OutboxifyProperties properties,
                ApplicationContext applicationContext) {

            KafkaTemplate<?, ?> defaultTemplate = null;

            // 1. Check if a default template reference is configured under defaults
            if (properties.getDefaults() != null
                    && properties.getDefaults().getBroker() != null
                    && properties.getDefaults().getBroker().getKafkaTemplateRef() != null) {
                String defaultRef = properties.getDefaults().getBroker().getKafkaTemplateRef();
                if (applicationContext.containsBean(defaultRef)) {
                    Object bean = applicationContext.getBean(defaultRef);
                    if (bean instanceof KafkaTemplate<?, ?> kt) {
                        defaultTemplate = kt;
                    }
                }
            }

            // 2. Try unique bean
            if (defaultTemplate == null) {
                try {
                    defaultTemplate = kafkaTemplateProvider.getIfUnique();
                } catch (Exception ignored) {
                    // ignore if resolution fails
                }
            }

            // 3. Try standard bean name "kafkaTemplate"
            if (defaultTemplate == null && applicationContext.containsBean("kafkaTemplate")) {
                Object bean = applicationContext.getBean("kafkaTemplate");
                if (bean instanceof KafkaTemplate<?, ?> kt) {
                    defaultTemplate = kt;
                }
            }

            // 4. Fallback to first available KafkaTemplate from stream
            if (defaultTemplate == null) {
                defaultTemplate = kafkaTemplateProvider.orderedStream().findFirst().orElse(null);
            }

            if (defaultTemplate == null) {
                throw new IllegalStateException("No KafkaTemplate bean available in ApplicationContext to configure Outboxify SpringKafkaBrokerPublisher");
            }

            log.info("Auto-configuring Outboxify SpringKafkaBrokerPublisher using Spring KafkaTemplate");
            SpringKafkaBrokerPublisher publisher = new SpringKafkaBrokerPublisher(defaultTemplate);

            if (properties.getPipelines() != null) {
                for (Map.Entry<String, OutboxifyProperties.PipelineConfigProps> entry : properties.getPipelines().entrySet()) {
                    String pipelineName = entry.getKey();
                    OutboxifyProperties.PipelineConfigProps pipelineProps = entry.getValue();
                    if (pipelineProps.getBroker() != null && pipelineProps.getBroker().getKafkaTemplateRef() != null) {
                        String templateRef = pipelineProps.getBroker().getKafkaTemplateRef();
                        if (applicationContext.containsBean(templateRef)) {
                            KafkaTemplate<?, ?> pipelineTemplate = (KafkaTemplate<?, ?>) applicationContext.getBean(templateRef);
                            publisher.registerPipelineTemplate(pipelineName, pipelineTemplate);
                            log.info("Registered pipeline-specific KafkaTemplate '{}' for pipeline '{}'", templateRef, pipelineName);
                        } else {
                            throw new IllegalArgumentException("Configured kafkaTemplateRef '" + templateRef + "' for pipeline '" + pipelineName + "' was not found in ApplicationContext");
                        }
                    }
                }
            }

            return publisher;
        }
    }

    /**
     * Fallback configuration when no KafkaTemplate or custom BrokerPublisher bean is defined.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnMissingBean(type = "org.springframework.kafka.core.KafkaTemplate")
    static class StandaloneKafkaPublisherConfiguration {

        @Bean
        @ConditionalOnMissingBean(BrokerPublisher.class)
        public BrokerPublisher openOutboxStandaloneKafkaBrokerPublisher(
                Map<String, PipelineConfig> openOutboxPipelineConfigs,
                ObjectProvider<Producer<String, String>> producerProvider) {

            Producer<String, String> defaultProducer = producerProvider.getIfAvailable();
            KafkaBrokerPublisher publisher;

            if (defaultProducer != null) {
                log.info("Auto-configuring Outboxify KafkaBrokerPublisher using provided Kafka Producer bean");
                publisher = new KafkaBrokerPublisher(defaultProducer);
            } else {
                BrokerConfig defaultBrokerConfig = openOutboxPipelineConfigs.values().stream()
                        .map(PipelineConfig::getBroker)
                        .findFirst()
                        .orElse(BrokerConfig.defaultConfig());

                log.info("Auto-configuring Outboxify KafkaBrokerPublisher with standalone broker config: bootstrap-servers={}",
                        defaultBrokerConfig.getBootstrapServers());
                publisher = new KafkaBrokerPublisher(defaultBrokerConfig);
            }

            for (PipelineConfig config : openOutboxPipelineConfigs.values()) {
                publisher.registerPipelineBroker(config.getName(), config.getBroker());
            }

            return publisher;
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxDispatcher openOutboxDispatcher(OutboxRepository openOutboxRepository, BrokerPublisher openOutboxBrokerPublisher) {
        return new OutboxDispatcher(openOutboxRepository, openOutboxBrokerPublisher);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxHook openOutboxHook(OutboxDispatcher openOutboxDispatcher, Map<String, PipelineConfig> openOutboxPipelineConfigs) {
        return new TransactionalOutboxHook(openOutboxDispatcher, openOutboxPipelineConfigs::get);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxPublisher openOutboxPublisher(OutboxRepository openOutboxRepository,
                                              OutboxHook openOutboxHook,
                                              OutboxDispatcher openOutboxDispatcher,
                                              Map<String, PipelineConfig> openOutboxPipelineConfigs) {
        return new SpringOutboxPublisher(openOutboxRepository, openOutboxHook, openOutboxDispatcher, openOutboxPipelineConfigs::get);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxifyLifecycleManager openOutboxLifecycleManager(Map<String, PipelineConfig> openOutboxPipelineConfigs,
                                                                OutboxRepository openOutboxRepository,
                                                                OutboxDispatcher openOutboxDispatcher,
                                                                BrokerPublisher openOutboxBrokerPublisher,
                                                                OutboxHook openOutboxHook) {
        return new OutboxifyLifecycleManager(openOutboxPipelineConfigs, openOutboxRepository, openOutboxDispatcher, openOutboxBrokerPublisher, openOutboxHook);
    }
}
