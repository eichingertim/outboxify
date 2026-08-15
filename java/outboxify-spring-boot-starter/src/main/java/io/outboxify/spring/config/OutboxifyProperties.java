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

package io.outboxify.spring.config;

import io.outboxify.core.model.BrokerConfig;
import io.outboxify.core.model.ColumnMapping;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.DialectType;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Hierarchical Spring Boot configuration properties mapped under 'outboxify.*'.
 */
@ConfigurationProperties(prefix = "outboxify")
public class OutboxifyProperties {

    private boolean enabled = true;
    private PipelineDefaults defaults = new PipelineDefaults();
    private Map<String, PipelineConfigProps> pipelines = new HashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public PipelineDefaults getDefaults() {
        return defaults;
    }

    public void setDefaults(PipelineDefaults defaults) {
        this.defaults = defaults;
    }

    public Map<String, PipelineConfigProps> getPipelines() {
        return pipelines;
    }

    public void setPipelines(Map<String, PipelineConfigProps> pipelines) {
        this.pipelines = pipelines;
    }

    /**
     * Converts a configured pipeline entry into an immutable domain {@link PipelineConfig},
     * merging global defaults with pipeline-specific overrides.
     */
    public PipelineConfig toPipelineConfig(String name, PipelineConfigProps props) {
        PipelineConfig.Builder builder = PipelineConfig.builder().name(name);

        if (props == null) {
            props = new PipelineConfigProps();
        }

        builder.enabled(props.getEnabled() != null ? props.getEnabled() : true);
        builder.tableName(props.getTableName() != null ? props.getTableName() : "OUTBOX_RECORD");
        builder.dialect(props.getDialect() != null ? props.getDialect() : DialectType.AUTO_DETECT);

        builder.batchSize(props.getBatchSize() != null ? props.getBatchSize() : defaults.getBatchSize());
        builder.pollIntervalMs(props.getPollIntervalMs() != null ? props.getPollIntervalMs() : defaults.getPollIntervalMs());
        builder.processingTimeoutSeconds(props.getProcessingTimeoutSeconds() != null ? props.getProcessingTimeoutSeconds() : defaults.getProcessingTimeoutSeconds());
        builder.reaperIntervalMs(props.getReaperIntervalMs() != null ? props.getReaperIntervalMs() : defaults.getReaperIntervalMs());
        builder.maxRetries(props.getMaxRetries() != null ? props.getMaxRetries() : defaults.getMaxRetries());
        builder.pollerThreads(props.getPollerThreads() != null ? props.getPollerThreads() : defaults.getPollerThreads());

        if (props.getImmediateSend() != null && props.getImmediateSend().getEnabled() != null) {
            builder.immediateSendEnabled(props.getImmediateSend().getEnabled());
        }

        // Column mapping
        if (props.getColumns() != null) {
            ColumnMappingProps cmp = props.getColumns();
            ColumnMapping.Builder colBuilder = ColumnMapping.builder();
            if (cmp.getId() != null) colBuilder.id(cmp.getId());
            if (cmp.getTopic() != null) colBuilder.topic(cmp.getTopic());
            if (cmp.getPartitionKey() != null) colBuilder.partitionKey(cmp.getPartitionKey());
            if (cmp.getPayload() != null) colBuilder.payload(cmp.getPayload());
            if (cmp.getHeaders() != null) colBuilder.headers(cmp.getHeaders());
            if (cmp.getStatus() != null) colBuilder.status(cmp.getStatus());
            if (cmp.getRetryCount() != null) colBuilder.retryCount(cmp.getRetryCount());
            if (cmp.getLastError() != null) colBuilder.lastError(cmp.getLastError());
            if (cmp.getCreatedAt() != null) colBuilder.createdAt(cmp.getCreatedAt());
            if (cmp.getUpdatedAt() != null) colBuilder.updatedAt(cmp.getUpdatedAt());
            if (cmp.getProcessedAt() != null) colBuilder.processedAt(cmp.getProcessedAt());
            builder.columns(colBuilder.build());
        }

        // Broker config
        if (props.getBroker() != null) {
            BrokerConfigProps bp = props.getBroker();
            BrokerConfig.Builder brokerBuilder = BrokerConfig.builder();
            if (bp.getType() != null) brokerBuilder.type(bp.getType());
            if (bp.getProducer() != null) {
                ProducerProps pp = bp.getProducer();
                if (pp.getBootstrapServers() != null) brokerBuilder.bootstrapServers(pp.getBootstrapServers());
                if (pp.getAcks() != null) brokerBuilder.acks(pp.getAcks());
                if (pp.getEnableIdempotence() != null) brokerBuilder.enableIdempotence(pp.getEnableIdempotence());
                if (pp.getLingerMs() != null) brokerBuilder.lingerMs(pp.getLingerMs());
                if (pp.getRetries() != null) brokerBuilder.retries(pp.getRetries());
                if (pp.getAdditionalProperties() != null) brokerBuilder.additionalProperties(pp.getAdditionalProperties());
            }
            builder.broker(brokerBuilder.build());
        }

        return builder.build();
    }

    public static class PipelineDefaults {
        private int batchSize = 100;
        private long pollIntervalMs = 1000L;
        private int processingTimeoutSeconds = 300;
        private long reaperIntervalMs = 10000L;
        private int maxRetries = 5;
        private int pollerThreads = 1;

        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        public long getPollIntervalMs() { return pollIntervalMs; }
        public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }
        public int getProcessingTimeoutSeconds() { return processingTimeoutSeconds; }
        public void setProcessingTimeoutSeconds(int processingTimeoutSeconds) { this.processingTimeoutSeconds = processingTimeoutSeconds; }
        public long getReaperIntervalMs() { return reaperIntervalMs; }
        public void setReaperIntervalMs(long reaperIntervalMs) { this.reaperIntervalMs = reaperIntervalMs; }
        public int getMaxRetries() { return maxRetries; }
        public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
        public int getPollerThreads() { return pollerThreads; }
        public void setPollerThreads(int pollerThreads) { this.pollerThreads = pollerThreads; }
    }

    public static class PipelineConfigProps {
        private Boolean enabled;
        private String tableName;
        private DialectType dialect;
        private Integer batchSize;
        private Long pollIntervalMs;
        private Integer processingTimeoutSeconds;
        private Long reaperIntervalMs;
        private Integer maxRetries;
        private Integer pollerThreads;
        private ImmediateSendProps immediateSend;
        private ColumnMappingProps columns;
        private BrokerConfigProps broker;

        public Boolean getEnabled() { return enabled; }
        public void setEnabled(Boolean enabled) { this.enabled = enabled; }
        public String getTableName() { return tableName; }
        public void setTableName(String tableName) { this.tableName = tableName; }
        public DialectType getDialect() { return dialect; }
        public void setDialect(DialectType dialect) { this.dialect = dialect; }
        public Integer getBatchSize() { return batchSize; }
        public void setBatchSize(Integer batchSize) { this.batchSize = batchSize; }
        public Long getPollIntervalMs() { return pollIntervalMs; }
        public void setPollIntervalMs(Long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }
        public Integer getProcessingTimeoutSeconds() { return processingTimeoutSeconds; }
        public void setProcessingTimeoutSeconds(Integer processingTimeoutSeconds) { this.processingTimeoutSeconds = processingTimeoutSeconds; }
        public Long getReaperIntervalMs() { return reaperIntervalMs; }
        public void setReaperIntervalMs(Long reaperIntervalMs) { this.reaperIntervalMs = reaperIntervalMs; }
        public Integer getMaxRetries() { return maxRetries; }
        public void setMaxRetries(Integer maxRetries) { this.maxRetries = maxRetries; }
        public Integer getPollerThreads() { return pollerThreads; }
        public void setPollerThreads(Integer pollerThreads) { this.pollerThreads = pollerThreads; }
        public ImmediateSendProps getImmediateSend() { return immediateSend; }
        public void setImmediateSend(ImmediateSendProps immediateSend) { this.immediateSend = immediateSend; }
        public ColumnMappingProps getColumns() { return columns; }
        public void setColumns(ColumnMappingProps columns) { this.columns = columns; }
        public BrokerConfigProps getBroker() { return broker; }
        public void setBroker(BrokerConfigProps broker) { this.broker = broker; }
    }

    public static class ImmediateSendProps {
        private Boolean enabled = true;
        private String mode = "TRANSACTION_HOOK";

        public Boolean getEnabled() { return enabled; }
        public void setEnabled(Boolean enabled) { this.enabled = enabled; }
        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
    }

    public static class ColumnMappingProps {
        private String id;
        private String topic;
        private String partitionKey;
        private String payload;
        private String headers;
        private String status;
        private String retryCount;
        private String lastError;
        private String createdAt;
        private String updatedAt;
        private String processedAt;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getPartitionKey() { return partitionKey; }
        public void setPartitionKey(String partitionKey) { this.partitionKey = partitionKey; }
        public String getPayload() { return payload; }
        public void setPayload(String payload) { this.payload = payload; }
        public String getHeaders() { return headers; }
        public void setHeaders(String headers) { this.headers = headers; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public String getRetryCount() { return retryCount; }
        public void setRetryCount(String retryCount) { this.retryCount = retryCount; }
        public String getLastError() { return lastError; }
        public void setLastError(String lastError) { this.lastError = lastError; }
        public String getCreatedAt() { return createdAt; }
        public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }
        public String getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(String updatedAt) { this.updatedAt = updatedAt; }
        public String getProcessedAt() { return processedAt; }
        public void setProcessedAt(String processedAt) { this.processedAt = processedAt; }
    }

    public static class BrokerConfigProps {
        private String type = "KAFKA";
        private ProducerProps producer = new ProducerProps();

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public ProducerProps getProducer() { return producer; }
        public void setProducer(ProducerProps producer) { this.producer = producer; }
    }

    public static class ProducerProps {
        private String bootstrapServers;
        private String acks = "all";
        private Boolean enableIdempotence = true;
        private Integer lingerMs = 5;
        private Integer retries = 3;
        private Map<String, String> additionalProperties = new HashMap<>();

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        public String getAcks() { return acks; }
        public void setAcks(String acks) { this.acks = acks; }
        public Boolean getEnableIdempotence() { return enableIdempotence; }
        public void setEnableIdempotence(Boolean enableIdempotence) { this.enableIdempotence = enableIdempotence; }
        public Integer getLingerMs() { return lingerMs; }
        public void setLingerMs(Integer lingerMs) { this.lingerMs = lingerMs; }
        public Integer getRetries() { return retries; }
        public void setRetries(Integer retries) { this.retries = retries; }
        public Map<String, String> getAdditionalProperties() { return additionalProperties; }
        public void setAdditionalProperties(Map<String, String> additionalProperties) { this.additionalProperties = additionalProperties; }
    }
}
