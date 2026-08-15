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

package io.outboxify.spring.lifecycle;

import io.outboxify.core.engine.OutboxDispatcher;
import io.outboxify.core.engine.OutboxHook;
import io.outboxify.core.engine.OutboxPollerEngine;
import io.outboxify.core.engine.OutboxReaperJob;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.BrokerPublisher;
import io.outboxify.core.spi.OutboxRepository;
import io.outboxify.spring.jpa.OutboxEntityListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the lifecycle of background poller engines and watchdog reaper jobs
 * across all configured pipelines within the Spring ApplicationContext.
 */
public class OutboxifyLifecycleManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxifyLifecycleManager.class);

    private final Map<String, PipelineConfig> pipelineConfigs;
    private final OutboxRepository repository;
    private final OutboxDispatcher dispatcher;
    private final BrokerPublisher brokerPublisher;
    private final OutboxHook outboxHook;

    private final List<OutboxPollerEngine> pollerEngines = new ArrayList<>();
    private final List<OutboxReaperJob> reaperJobs = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public OutboxifyLifecycleManager(Map<String, PipelineConfig> pipelineConfigs,
                                      OutboxRepository repository,
                                      OutboxDispatcher dispatcher,
                                      BrokerPublisher brokerPublisher,
                                      OutboxHook outboxHook) {
        this.pipelineConfigs = pipelineConfigs;
        this.repository = repository;
        this.dispatcher = dispatcher;
        this.brokerPublisher = brokerPublisher;
        this.outboxHook = outboxHook;
    }

    @Override
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Initializing Outboxify pipelines ({} registered)", pipelineConfigs.size());

            // Bind JPA listener hook
            OutboxEntityListener.setOutboxHook(outboxHook);

            for (PipelineConfig config : pipelineConfigs.values()) {
                if (!config.isEnabled()) {
                    log.info("Pipeline '{}' is disabled, skipping schedulers", config.getName());
                    continue;
                }

                // Initialize and start Slow-Path Poller
                OutboxPollerEngine poller = new OutboxPollerEngine(config, repository, dispatcher);
                poller.start();
                pollerEngines.add(poller);

                // Initialize and start Watchdog Reaper
                OutboxReaperJob reaper = new OutboxReaperJob(config, repository);
                reaper.start();
                reaperJobs.add(reaper);

                log.info("Initialized poller and reaper for Outboxify pipeline '{}' (table='{}', dialect={})",
                        config.getName(), config.getTableName(), config.getDialect());
            }
        }
    }

    @Override
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping Outboxify lifecycle services...");

            for (OutboxPollerEngine poller : pollerEngines) {
                try {
                    poller.stop();
                } catch (Exception e) {
                    log.warn("Error stopping poller engine: {}", e.getMessage());
                }
            }
            pollerEngines.clear();

            for (OutboxReaperJob reaper : reaperJobs) {
                try {
                    reaper.stop();
                } catch (Exception e) {
                    log.warn("Error stopping reaper job: {}", e.getMessage());
                }
            }
            reaperJobs.clear();

            try {
                brokerPublisher.close();
            } catch (Exception e) {
                log.warn("Error closing broker publisher: {}", e.getMessage());
            }

            log.info("Outboxify services stopped successfully");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    public List<OutboxPollerEngine> getPollerEngines() {
        return pollerEngines;
    }

    public List<OutboxReaperJob> getReaperJobs() {
        return reaperJobs;
    }
}
