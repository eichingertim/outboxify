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

import java.util.Objects;

/**
 * Maps physical database table columns to logical outbox record properties.
 * Allows seamless integration with existing domain tables (e.g. ORDERS)
 * or custom dedicated outbox tables.
 */
public final class ColumnMapping {

    public static final String DEFAULT_ID = "id";
    public static final String DEFAULT_TOPIC = "topic";
    public static final String DEFAULT_PARTITION_KEY = "partition_key";
    public static final String DEFAULT_PAYLOAD = "payload";
    public static final String DEFAULT_HEADERS = "headers";
    public static final String DEFAULT_STATUS = "status";
    public static final String DEFAULT_RETRY_COUNT = "retry_count";
    public static final String DEFAULT_LAST_ERROR = "last_error";
    public static final String DEFAULT_CREATED_AT = "created_at";
    public static final String DEFAULT_UPDATED_AT = "updated_at";
    public static final String DEFAULT_PROCESSED_AT = "processed_at";

    private final String id;
    private final String topic;
    private final String partitionKey;
    private final String payload;
    private final String headers;
    private final String status;
    private final String retryCount;
    private final String lastError;
    private final String createdAt;
    private final String updatedAt;
    private final String processedAt;

    private ColumnMapping(Builder builder) {
        this.id = defaultIfBlank(builder.id, DEFAULT_ID);
        this.topic = defaultIfBlank(builder.topic, DEFAULT_TOPIC);
        this.partitionKey = defaultIfBlank(builder.partitionKey, DEFAULT_PARTITION_KEY);
        this.payload = defaultIfBlank(builder.payload, DEFAULT_PAYLOAD);
        this.headers = defaultIfBlank(builder.headers, DEFAULT_HEADERS);
        this.status = defaultIfBlank(builder.status, DEFAULT_STATUS);
        this.retryCount = defaultIfBlank(builder.retryCount, DEFAULT_RETRY_COUNT);
        this.lastError = defaultIfBlank(builder.lastError, DEFAULT_LAST_ERROR);
        this.createdAt = defaultIfBlank(builder.createdAt, DEFAULT_CREATED_AT);
        this.updatedAt = defaultIfBlank(builder.updatedAt, DEFAULT_UPDATED_AT);
        this.processedAt = defaultIfBlank(builder.processedAt, DEFAULT_PROCESSED_AT);
    }

    public static ColumnMapping defaultMapping() {
        return new Builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private static String defaultIfBlank(String value, String defaultValue) {
        return (value != null && !value.trim().isEmpty()) ? value.trim() : defaultValue;
    }

    public String getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public String getPayload() {
        return payload;
    }

    public String getHeaders() {
        return headers;
    }

    public String getStatus() {
        return status;
    }

    public String getRetryCount() {
        return retryCount;
    }

    public String getLastError() {
        return lastError;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public String getProcessedAt() {
        return processedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnMapping that = (ColumnMapping) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(partitionKey, that.partitionKey) &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(headers, that.headers) &&
                Objects.equals(status, that.status) &&
                Objects.equals(retryCount, that.retryCount) &&
                Objects.equals(lastError, that.lastError) &&
                Objects.equals(createdAt, that.createdAt) &&
                Objects.equals(updatedAt, that.updatedAt) &&
                Objects.equals(processedAt, that.processedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, topic, partitionKey, payload, headers, status, retryCount, lastError, createdAt, updatedAt, processedAt);
    }

    @Override
    public String toString() {
        return "ColumnMapping{" +
                "id='" + id + '\'' +
                ", topic='" + topic + '\'' +
                ", partitionKey='" + partitionKey + '\'' +
                ", payload='" + payload + '\'' +
                ", status='" + status + '\'' +
                '}';
    }

    public static class Builder {
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

        public Builder id(String id) {
            this.id = id;
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

        public Builder headers(String headers) {
            this.headers = headers;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public Builder retryCount(String retryCount) {
            this.retryCount = retryCount;
            return this;
        }

        public Builder lastError(String lastError) {
            this.lastError = lastError;
            return this;
        }

        public Builder createdAt(String createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder updatedAt(String updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder processedAt(String processedAt) {
            this.processedAt = processedAt;
            return this;
        }

        public ColumnMapping build() {
            return new ColumnMapping(this);
        }
    }
}
