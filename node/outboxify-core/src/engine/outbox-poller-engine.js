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

class OutboxPollerEngine {
  /**
   * @param {import('../model/pipeline-config').PipelineConfig} config
   * @param {import('../spi/outbox-repository').OutboxRepository} repository
   * @param {import('./outbox-dispatcher').OutboxDispatcher} dispatcher
   */
  constructor(config, repository, dispatcher) {
    this.config = config;
    this.repository = repository;
    this.dispatcher = dispatcher;
    this.timer = null;
    this.running = false;
    this.polling = false;
  }

  start() {
    if (this.running) return;
    this.running = true;

    const scheduleNext = () => {
      if (!this.running) return;
      this.timer = setTimeout(async () => {
        try {
          await this.pollOnce();
        } catch (err) {
          // ignore poller cycle errors
        } finally {
          scheduleNext();
        }
      }, this.config.pollIntervalMs);
    };

    scheduleNext();
  }

  stop() {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  async pollOnce() {
    if (!this.config.enabled || this.polling) {
      return 0;
    }

    this.polling = true;
    try {
      const records = await this.repository.fetchBatchForUpdate(this.config, this.config.batchSize);
      if (!records || records.length === 0) {
        return 0;
      }

      await this.dispatcher.dispatchBatch(this.config, records);
      return records.length;
    } finally {
      this.polling = false;
    }
  }
}

module.exports = {
  OutboxPollerEngine
};
