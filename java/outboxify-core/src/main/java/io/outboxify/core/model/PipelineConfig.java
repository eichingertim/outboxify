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

package io.outboxify.core.model;

import io.outboxify.core.spi.DialectType;

import java.util.Objects;

/**
 * Pipeline configuration holding table mappings, polling cadence, retry ceilings, and broker settings.
 */
public final class PipelineConfig {

    private final String name;
    private final boolean enabled;
    private final String tableName;
    private final DialectType dialect;
    private final int batchSize;
    private final long pollIntervalMs;
    private final int processingTimeoutSeconds;
    private final long reaperIntervalMs;
    private final int maxRetries;
    private final int pollerThreads;
    private final boolean immediateSendEnabled;
    private final ColumnMapping columns;
    private final BrokerConfig broker;

    private PipelineConfig(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "pipeline name must not be null");
        this.enabled = builder.enabled;
        this.tableName = Objects.requireNonNull(builder.tableName, "tableName must not be null");
        this.dialect = builder.dialect != null ? builder.dialect : DialectType.AUTO_DETECT;
        this.batchSize = builder.batchSize > 0 ? builder.batchSize : 100;
        this.pollIntervalMs = builder.pollIntervalMs > 0 ? builder.pollIntervalMs : 1000L;
        this.processingTimeoutSeconds = builder.processingTimeoutSeconds > 0 ? builder.processingTimeoutSeconds : 300;
        this.reaperIntervalMs = builder.reaperIntervalMs > 0 ? builder.reaperIntervalMs : 10000L;
        this.maxRetries = builder.maxRetries >= 0 ? builder.maxRetries : 5;
        this.pollerThreads = builder.pollerThreads > 0 ? builder.pollerThreads : 1;
        this.immediateSendEnabled = builder.immediateSendEnabled;
        this.columns = builder.columns != null ? builder.columns : ColumnMapping.defaultMapping();
        this.broker = builder.broker != null ? builder.broker : BrokerConfig.defaultConfig();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getName() {
        return name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getTableName() {
        return tableName;
    }

    public DialectType getDialect() {
        return dialect;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    public int getProcessingTimeoutSeconds() {
        return processingTimeoutSeconds;
    }

    public long getReaperIntervalMs() {
        return reaperIntervalMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getPollerThreads() {
        return pollerThreads;
    }

    public boolean isImmediateSendEnabled() {
        return immediateSendEnabled;
    }

    public ColumnMapping getColumns() {
        return columns;
    }

    public BrokerConfig getBroker() {
        return broker;
    }

    @Override
    public String toString() {
        return "PipelineConfig{" +
                "name='" + name + '\'' +
                ", tableName='" + tableName + '\'' +
                ", dialect=" + dialect +
                ", batchSize=" + batchSize +
                ", pollIntervalMs=" + pollIntervalMs +
                '}';
    }

    public static class Builder {
        private String name = "default";
        private boolean enabled = true;
        private String tableName = "OUTBOX_RECORD";
        private DialectType dialect = DialectType.AUTO_DETECT;
        private int batchSize = 100;
        private long pollIntervalMs = 1000L;
        private int processingTimeoutSeconds = 300;
        private long reaperIntervalMs = 10000L;
        private int maxRetries = 5;
        private int pollerThreads = 1;
        private boolean immediateSendEnabled = true;
        private ColumnMapping columns = ColumnMapping.defaultMapping();
        private BrokerConfig broker = BrokerConfig.defaultConfig();

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder dialect(DialectType dialect) {
            this.dialect = dialect;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder pollIntervalMs(long pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
            return this;
        }

        public Builder processingTimeoutSeconds(int processingTimeoutSeconds) {
            this.processingTimeoutSeconds = processingTimeoutSeconds;
            return this;
        }

        public Builder reaperIntervalMs(long reaperIntervalMs) {
            this.reaperIntervalMs = reaperIntervalMs;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder pollerThreads(int pollerThreads) {
            this.pollerThreads = pollerThreads;
            return this;
        }

        public Builder immediateSendEnabled(boolean immediateSendEnabled) {
            this.immediateSendEnabled = immediateSendEnabled;
            return this;
        }

        public Builder columns(ColumnMapping columns) {
            this.columns = columns;
            return this;
        }

        public Builder broker(BrokerConfig broker) {
            this.broker = broker;
            return this;
        }

        public PipelineConfig build() {
            return new PipelineConfig(this);
        }
    }
}
