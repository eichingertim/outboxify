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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Message broker connection and publisher configuration parameters.
 */
public final class BrokerConfig {

    private final String type;
    private final String bootstrapServers;
    private final String acks;
    private final boolean enableIdempotence;
    private final int lingerMs;
    private final int retries;
    private final Map<String, String> additionalProperties;

    private BrokerConfig(Builder builder) {
        this.type = builder.type != null ? builder.type : "KAFKA";
        this.bootstrapServers = builder.bootstrapServers;
        this.acks = builder.acks != null ? builder.acks : "all";
        this.enableIdempotence = builder.enableIdempotence;
        this.lingerMs = builder.lingerMs;
        this.retries = builder.retries;
        this.additionalProperties = builder.additionalProperties == null ?
                Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(builder.additionalProperties));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static BrokerConfig defaultConfig() {
        return new Builder().build();
    }

    public String getType() {
        return type;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getAcks() {
        return acks;
    }

    public boolean isEnableIdempotence() {
        return enableIdempotence;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public int getRetries() {
        return retries;
    }

    public Map<String, String> getAdditionalProperties() {
        return additionalProperties;
    }

    public static class Builder {
        private String type = "KAFKA";
        private String bootstrapServers;
        private String acks = "all";
        private boolean enableIdempotence = true;
        private int lingerMs = 5;
        private int retries = 3;
        private Map<String, String> additionalProperties = new HashMap<>();

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder acks(String acks) {
            this.acks = acks;
            return this;
        }

        public Builder enableIdempotence(boolean enableIdempotence) {
            this.enableIdempotence = enableIdempotence;
            return this;
        }

        public Builder lingerMs(int lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public Builder retries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder additionalProperties(Map<String, String> additionalProperties) {
            if (additionalProperties != null) {
                this.additionalProperties = new HashMap<>(additionalProperties);
            }
            return this;
        }

        public Builder property(String key, String value) {
            if (this.additionalProperties == null) {
                this.additionalProperties = new HashMap<>();
            }
            this.additionalProperties.put(key, value);
            return this;
        }

        public BrokerConfig build() {
            return new BrokerConfig(this);
        }
    }
}
