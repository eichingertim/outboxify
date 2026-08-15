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

class OutboxPayload {
  /**
   * @param {Object} options
   * @param {string} options.topic
   * @param {string} [options.partitionKey]
   * @param {string} options.payload
   * @param {Record<string, string>} [options.headers]
   */
  constructor({ topic, partitionKey = null, payload, headers = {} }) {
    if (!topic) throw new Error('topic is required');
    if (payload === undefined || payload === null) throw new Error('payload is required');

    this.topic = topic;
    this.partitionKey = partitionKey;
    this.payload = typeof payload === 'string' ? payload : JSON.stringify(payload);
    this.headers = headers || {};
  }

  static of(topic, payload, partitionKey = null, headers = {}) {
    return new OutboxPayload({ topic, payload, partitionKey, headers });
  }
}

module.exports = {
  OutboxPayload
};
