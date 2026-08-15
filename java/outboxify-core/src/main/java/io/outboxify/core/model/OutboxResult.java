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
 * Result of publishing an outbox record to the broker.
 */
public final class OutboxResult {

    private final String recordId;
    private final OutboxStatus status;
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final String errorMessage;
    private final Throwable throwable;

    private OutboxResult(Builder builder) {
        this.recordId = Objects.requireNonNull(builder.recordId, "recordId must not be null");
        this.status = Objects.requireNonNull(builder.status, "status must not be null");
        this.topic = builder.topic;
        this.partition = builder.partition;
        this.offset = builder.offset;
        this.timestamp = builder.timestamp > 0 ? builder.timestamp : System.currentTimeMillis();
        this.errorMessage = builder.errorMessage;
        this.throwable = builder.throwable;
    }

    public static OutboxResult success(String recordId, String topic, int partition, long offset) {
        return new Builder()
                .recordId(recordId)
                .status(OutboxStatus.SENT)
                .topic(topic)
                .partition(partition)
                .offset(offset)
                .build();
    }

    public static OutboxResult failure(String recordId, String topic, String errorMessage, Throwable throwable) {
        return new Builder()
                .recordId(recordId)
                .status(OutboxStatus.FAILED)
                .topic(topic)
                .errorMessage(errorMessage != null ? errorMessage : (throwable != null ? throwable.getMessage() : "Unknown error"))
                .throwable(throwable)
                .build();
    }

    public String getRecordId() {
        return recordId;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public boolean isSuccess() {
        return status == OutboxStatus.SENT;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public String toString() {
        return "OutboxResult{" +
                "recordId='" + recordId + '\'' +
                ", status=" + status +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }

    public static class Builder {
        private String recordId;
        private OutboxStatus status = OutboxStatus.NEW;
        private String topic;
        private int partition = -1;
        private long offset = -1;
        private long timestamp = System.currentTimeMillis();
        private String errorMessage;
        private Throwable throwable;

        public Builder recordId(String recordId) {
            this.recordId = recordId;
            return this;
        }

        public Builder status(OutboxStatus status) {
            this.status = status;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder throwable(Throwable throwable) {
            this.throwable = throwable;
            return this;
        }

        public OutboxResult build() {
            return new OutboxResult(this);
        }
    }
}
