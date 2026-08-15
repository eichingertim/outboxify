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

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Standard immutable implementation of {@link OutboxRecord}.
 */
public final class DefaultOutboxRecord implements OutboxRecord {

    private final String outboxPipeline;
    private final String outboxId;
    private final String topic;
    private final String partitionKey;
    private final String payload;
    private final Map<String, String> headers;
    private final OutboxStatus status;
    private final int retryCount;
    private final String lastError;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final Instant processedAt;

    private DefaultOutboxRecord(Builder builder) {
        this.outboxPipeline = builder.outboxPipeline != null ? builder.outboxPipeline : "default";
        this.outboxId = builder.outboxId != null ? builder.outboxId : UUID.randomUUID().toString();
        this.topic = Objects.requireNonNull(builder.topic, "topic must not be null");
        this.partitionKey = builder.partitionKey;
        this.payload = Objects.requireNonNull(builder.payload, "payload must not be null");
        this.headers = builder.headers == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(builder.headers));
        this.status = builder.status != null ? builder.status : OutboxStatus.NEW;
        this.retryCount = builder.retryCount;
        this.lastError = builder.lastError;
        this.createdAt = builder.createdAt != null ? builder.createdAt : Instant.now();
        this.updatedAt = builder.updatedAt != null ? builder.updatedAt : Instant.now();
        this.processedAt = builder.processedAt;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DefaultOutboxRecord fromPayload(String pipeline, String id, OutboxPayload payload) {
        return builder()
                .outboxPipeline(pipeline)
                .outboxId(id)
                .topic(payload.getTopic())
                .partitionKey(payload.getPartitionKey())
                .payload(payload.getPayload())
                .headers(payload.getHeaders())
                .status(OutboxStatus.NEW)
                .build();
    }

    @Override
    public String getOutboxPipeline() {
        return outboxPipeline;
    }

    @Override
    public String getOutboxId() {
        return outboxId;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public String getPartitionKey() {
        return partitionKey;
    }

    @Override
    public String getPayload() {
        return payload;
    }

    @Override
    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public OutboxStatus getStatus() {
        return status;
    }

    @Override
    public int getRetryCount() {
        return retryCount;
    }

    @Override
    public String getLastError() {
        return lastError;
    }

    @Override
    public Instant getCreatedAt() {
        return createdAt;
    }

    @Override
    public Instant getUpdatedAt() {
        return updatedAt;
    }

    @Override
    public Instant getProcessedAt() {
        return processedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultOutboxRecord that = (DefaultOutboxRecord) o;
        return Objects.equals(outboxId, that.outboxId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outboxId);
    }

    @Override
    public String toString() {
        return "DefaultOutboxRecord{" +
                "pipeline='" + outboxPipeline + '\'' +
                ", id='" + outboxId + '\'' +
                ", topic='" + topic + '\'' +
                ", status=" + status +
                ", retryCount=" + retryCount +
                '}';
    }

    public static class Builder {
        private String outboxPipeline = "default";
        private String outboxId;
        private String topic;
        private String partitionKey;
        private String payload;
        private Map<String, String> headers = new HashMap<>();
        private OutboxStatus status = OutboxStatus.NEW;
        private int retryCount = 0;
        private String lastError;
        private Instant createdAt = Instant.now();
        private Instant updatedAt = Instant.now();
        private Instant processedAt;

        public Builder outboxPipeline(String outboxPipeline) {
            this.outboxPipeline = outboxPipeline;
            return this;
        }

        public Builder outboxId(String outboxId) {
            this.outboxId = outboxId;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder partitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public Builder payload(String payload) {
            this.payload = payload;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            if (headers != null) {
                this.headers = new HashMap<>(headers);
            }
            return this;
        }

        public Builder header(String key, String value) {
            if (this.headers == null) {
                this.headers = new HashMap<>();
            }
            this.headers.put(key, value);
            return this;
        }

        public Builder status(OutboxStatus status) {
            this.status = status;
            return this;
        }

        public Builder retryCount(int retryCount) {
            this.retryCount = retryCount;
            return this;
        }

        public Builder lastError(String lastError) {
            this.lastError = lastError;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder updatedAt(Instant updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder processedAt(Instant processedAt) {
            this.processedAt = processedAt;
            return this;
        }

        public DefaultOutboxRecord build() {
            return new DefaultOutboxRecord(this);
        }
    }
}
