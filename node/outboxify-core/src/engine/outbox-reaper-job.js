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

class OutboxReaperJob {
  /**
   * @param {import('../model/pipeline-config').PipelineConfig} config
   * @param {import('../spi/outbox-repository').OutboxRepository} repository
   */
  constructor(config, repository) {
    this.config = config;
    this.repository = repository;
    this.timer = null;
    this.running = false;
  }

  start() {
    if (this.running) return;
    this.running = true;

    const scheduleNext = () => {
      if (!this.running) return;
      this.timer = setTimeout(async () => {
        try {
          await this.reapOnce();
        } catch (err) {
          // ignore watchdog cycle errors
        } finally {
          scheduleNext();
        }
      }, this.config.reaperIntervalMs);
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

  async reapOnce() {
    if (!this.config.enabled) {
      return 0;
    }
    return this.repository.reapStaleRecords(
      this.config,
      this.config.processingTimeoutSeconds,
      this.config.maxRetries
    );
  }
}

module.exports = {
  OutboxReaperJob
};
