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

'use strict';

const { OutboxStatus } = require('./outbox-status');

class OutboxResult {
  constructor({ recordId, status, topic, partition = -1, offset = -1, timestamp = Date.now(), errorMessage = null, error = null }) {
    this.recordId = recordId;
    this.status = status;
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.errorMessage = errorMessage;
    this.error = error;
  }

  isSuccess() {
    return this.status === OutboxStatus.SENT;
  }

  static success(recordId, topic, partition = 0, offset = 0) {
    return new OutboxResult({
      recordId,
      status: OutboxStatus.SENT,
      topic,
      partition,
      offset
    });
  }

  static failure(recordId, topic, errorMessage, error = null) {
    return new OutboxResult({
      recordId,
      status: OutboxStatus.FAILED,
      topic,
      errorMessage,
      error
    });
  }
}

module.exports = {
  OutboxResult
};
