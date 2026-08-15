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
import java.util.Objects;

/**
 * Encapsulates the message content and metadata to be staged or published to the broker.
 */
public final class OutboxPayload {

    private final String topic;
    private final String partitionKey;
    private final String payload;
    private final Map<String, String> headers;

    private OutboxPayload(Builder builder) {
        this.topic = Objects.requireNonNull(builder.topic, "topic must not be null");
        this.partitionKey = builder.partitionKey;
        this.payload = Objects.requireNonNull(builder.payload, "payload must not be null");
        this.headers = builder.headers == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(builder.headers));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static OutboxPayload of(String topic, String payload) {
        return builder().topic(topic).payload(payload).build();
    }

    public static OutboxPayload of(String topic, String partitionKey, String payload) {
        return builder().topic(topic).partitionKey(partitionKey).payload(payload).build();
    }

    public static OutboxPayload of(String topic, String partitionKey, String payload, Map<String, String> headers) {
        return builder().topic(topic).partitionKey(partitionKey).payload(payload).headers(headers).build();
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

    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutboxPayload that = (OutboxPayload) o;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(partitionKey, that.partitionKey) &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partitionKey, payload, headers);
    }

    @Override
    public String toString() {
        return "OutboxPayload{" +
                "topic='" + topic + '\'' +
                ", partitionKey='" + partitionKey + '\'' +
                ", payloadLength=" + (payload != null ? payload.length() : 0) +
                ", headers=" + headers +
                '}';
    }

    public static class Builder {
        private String topic;
        private String partitionKey;
        private String payload;
        private Map<String, String> headers = new HashMap<>();

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

        public OutboxPayload build() {
            return new OutboxPayload(this);
        }
    }
}
