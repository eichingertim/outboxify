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

package io.outboxify.core.engine;

import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background poller engine (Slow-Path) that queries pending outbox records using SKIP LOCKED,
 * claims them into PROCESSING state, and dispatches them in batches to the broker.
 */
public class OutboxPollerEngine implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OutboxPollerEngine.class);

    private final PipelineConfig config;
    private final OutboxRepository repository;
    private final OutboxDispatcher dispatcher;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public OutboxPollerEngine(PipelineConfig config, OutboxRepository repository, OutboxDispatcher dispatcher) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher must not be null");
        int threads = Math.max(1, config.getPollerThreads());
        this.scheduler = Executors.newScheduledThreadPool(threads, r -> {
            Thread t = new Thread(r, "outboxify-poller-" + config.getName() + "-" + System.identityHashCode(r));
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the polling schedule.
     */
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting Outboxify poller engine for pipeline '{}' with interval {}ms, batchSize={}",
                    config.getName(), config.getPollIntervalMs(), config.getBatchSize());
            scheduler.scheduleWithFixedDelay(
                    this::pollOnceSafe,
                    config.getPollIntervalMs(),
                    config.getPollIntervalMs(),
                    TimeUnit.MILLISECONDS
            );
        }
    }

    /**
     * Stops the poller scheduler and shuts down threads.
     */
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping Outboxify poller engine for pipeline '{}'", config.getName());
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Executes a single polling iteration wrapped in exception safety.
     *
     * @return number of records processed in this iteration
     */
    public int pollOnce() {
        if (!config.isEnabled()) {
            return 0;
        }

        try {
            // Atomically fetch and claim eligible batch in PROCESSING status
            List<OutboxRecord> records = repository.fetchBatchForUpdate(config, config.getBatchSize());
            if (records.isEmpty()) {
                log.trace("Poller pipeline '{}' found no pending records", config.getName());
                return 0;
            }

            log.debug("Poller pipeline '{}' fetched and claimed {} records for dispatch", config.getName(), records.size());

            // Dispatch batch asynchronously to message broker
            dispatcher.dispatchBatch(config, records);

            return records.size();
        } catch (Exception e) {
            log.warn("Poller cycle deferred for pipeline '{}': {}", config.getName(), e.getMessage());
            return 0;
        }
    }

    private void pollOnceSafe() {
        try {
            pollOnce();
        } catch (Throwable t) {
            log.warn("Poller execution error for pipeline '{}': {}", config.getName(), t.getMessage());
        }
    }

    @Override
    public void close() {
        stop();
    }
}
