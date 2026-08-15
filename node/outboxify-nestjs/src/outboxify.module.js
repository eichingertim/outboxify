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

const {
  PipelineConfig,
  OutboxDispatcher,
  OutboxPollerEngine,
  OutboxReaperJob,
  OutboxHook,
  OutboxPublisher
} = require('../../outboxify-core/src/index');

class OutboxifyService {
  /**
   * @param {Object} options
   * @param {Record<string, PipelineConfig>} options.pipelines
   * @param {import('../../outboxify-core/src/spi/outbox-repository').OutboxRepository} options.repository
   * @param {import('../../outboxify-core/src/spi/broker-publisher').BrokerPublisher} options.brokerPublisher
   */
  constructor({ pipelines, repository, brokerPublisher }) {
    this.pipelines = new Map();
    for (const [name, cfg] of Object.entries(pipelines || {})) {
      this.pipelines.set(name, cfg instanceof PipelineConfig ? cfg : new PipelineConfig({ ...cfg, name }));
    }
    if (this.pipelines.size === 0) {
      this.pipelines.set('default', new PipelineConfig({ name: 'default' }));
    }

    this.repository = repository;
    this.brokerPublisher = brokerPublisher;

    this.dispatcher = new OutboxDispatcher(this.repository, this.brokerPublisher);
    this.hook = new OutboxHook(this.dispatcher, (name) => this.pipelines.get(name));
    this.publisher = new OutboxPublisher(this.repository, this.hook, this.dispatcher, (name) => this.pipelines.get(name));

    this.pollers = [];
    this.reapers = [];

    for (const config of this.pipelines.values()) {
      if (config.enabled) {
        this.pollers.push(new OutboxPollerEngine(config, this.repository, this.dispatcher));
        this.reapers.push(new OutboxReaperJob(config, this.repository));
      }
    }
  }

  onModuleInit() {
    for (const p of this.pollers) p.start();
    for (const r of this.reapers) r.start();
  }

  async onModuleDestroy() {
    for (const p of this.pollers) p.stop();
    for (const r of this.reapers) r.stop();
    if (this.brokerPublisher && typeof this.brokerPublisher.close === 'function') {
      await this.brokerPublisher.close();
    }
  }

  async publish(pipeline, payload, tx = null) {
    return this.publisher.publish(pipeline, payload, tx);
  }
}

class OutboxifyModule {
  static forRoot(options) {
    return {
      module: OutboxifyModule,
      providers: [
        {
          provide: OutboxifyService,
          useFactory: () => new OutboxifyService(options)
        }
      ],
      exports: [OutboxifyService]
    };
  }
}

module.exports = {
  OutboxifyService,
  OutboxifyModule
};
