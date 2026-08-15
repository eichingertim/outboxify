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

import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Watchdog job that periodically discovers stuck or orphaned PROCESSING outbox rows
 * (caused by pod crashes, network partitions, or ungraceful terminations)
 * and safely recovers them back to FAILED / NEW state.
 */
public class OutboxReaperJob implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OutboxReaperJob.class);

    private final PipelineConfig config;
    private final OutboxRepository repository;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public OutboxReaperJob(PipelineConfig config, OutboxRepository repository) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outboxify-reaper-" + config.getName());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the scheduled reaper watchdog.
     */
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting Outboxify watchdog reaper for pipeline '{}' with interval {}ms, timeout={}s",
                    config.getName(), config.getReaperIntervalMs(), config.getProcessingTimeoutSeconds());
            scheduler.scheduleWithFixedDelay(
                    this::reapOnceSafe,
                    config.getReaperIntervalMs(),
                    config.getReaperIntervalMs(),
                    TimeUnit.MILLISECONDS
            );
        }
    }

    /**
     * Stops the reaper scheduler.
     */
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping Outboxify reaper job for pipeline '{}'", config.getName());
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
     * Runs a single reaper recovery sweep.
     *
     * @return number of stale records recovered
     */
    public int reapOnce() {
        if (!config.isEnabled()) {
            return 0;
        }

        try {
            int reapedCount = repository.reapStaleRecords(
                    config,
                    config.getProcessingTimeoutSeconds(),
                    config.getMaxRetries()
            );

            if (reapedCount > 0) {
                log.warn("Watchdog reaper recovered {} stale PROCESSING records on pipeline '{}'", reapedCount, config.getName());
            } else {
                log.trace("Watchdog reaper found 0 stale records on pipeline '{}'", config.getName());
            }

            return reapedCount;
        } catch (Exception e) {
            log.error("Error during reaper execution for pipeline '{}': {}", config.getName(), e.getMessage(), e);
            return 0;
        }
    }

    private void reapOnceSafe() {
        try {
            reapOnce();
        } catch (Throwable t) {
            log.error("Unexpected error in reaper thread for pipeline '{}'", config.getName(), t);
        }
    }

    @Override
    public void close() {
        stop();
    }
}
