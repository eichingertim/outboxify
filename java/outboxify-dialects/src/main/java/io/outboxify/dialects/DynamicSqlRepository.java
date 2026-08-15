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

package io.outboxify.dialects;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.outboxify.core.model.ColumnMapping;
import io.outboxify.core.model.DefaultOutboxRecord;
import io.outboxify.core.model.OutboxRecord;
import io.outboxify.core.model.OutboxStatus;
import io.outboxify.core.model.PipelineConfig;
import io.outboxify.core.spi.ConnectionProvider;
import io.outboxify.core.spi.DatabaseDialect;
import io.outboxify.core.spi.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dynamic, precompiled, high-throughput JDBC repository executing dialect-optimized SQL
 * with batching support for Oracle, PostgreSQL, MySQL, and MS SQL Server.
 */
public class DynamicSqlRepository implements OutboxRepository {

    private static final Logger log = LoggerFactory.getLogger(DynamicSqlRepository.class);

    private final DataSource dataSource;
    private final ConnectionProvider connectionProvider;
    private final DialectRegistry dialectRegistry;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Cache precompiled SQL queries per pipeline
    private final Map<String, SqlStatements> statementCache = new ConcurrentHashMap<>();

    public DynamicSqlRepository(DataSource dataSource, DialectRegistry dialectRegistry) {
        this(dataSource, dataSource::getConnection, dialectRegistry);
    }

    public DynamicSqlRepository(DataSource dataSource, ConnectionProvider connectionProvider, DialectRegistry dialectRegistry) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider must not be null");
        this.dialectRegistry = Objects.requireNonNull(dialectRegistry, "dialectRegistry must not be null");
    }

    private SqlStatements getOrCreateStatements(PipelineConfig config) {
        return statementCache.computeIfAbsent(config.getName(), name -> {
            DatabaseDialect dialect = dialectRegistry.resolveDialect(config.getDialect(), dataSource);
            return SqlStatements.build(config, dialect);
        });
    }

    @Override
    public List<OutboxRecord> fetchBatchForUpdate(PipelineConfig config, int batchSize) {
        SqlStatements sql = getOrCreateStatements(config);
        List<OutboxRecord> records = new ArrayList<>();

        try (Connection connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement ps = connection.prepareStatement(sql.selectBatch)) {
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            records.add(mapRowToRecord(rs, config.getName()));
                        }
                    }
                }

                // Claim the locked batch immediately in the same transaction to guarantee no duplicate pickups
                if (!records.isEmpty()) {
                    try (PreparedStatement psUpdate = connection.prepareStatement(sql.updateProcessing)) {
                        for (OutboxRecord record : records) {
                            psUpdate.setString(1, record.getOutboxId());
                            psUpdate.addBatch();
                        }
                        psUpdate.executeBatch();
                    }
                }

                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            log.error("Error executing fetchBatchForUpdate on pipeline '{}': {}", config.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to fetch batch for update", e);
        }

        return records;
    }

    @Override
    public int markProcessing(PipelineConfig config, List<String> recordIds) {
        if (recordIds == null || recordIds.isEmpty()) {
            return 0;
        }

        SqlStatements sql = getOrCreateStatements(config);
        Connection connection = null;
        try {
            connection = connectionProvider.getConnection();
            try (PreparedStatement ps = connection.prepareStatement(sql.updateProcessing)) {
                for (String id : recordIds) {
                    ps.setString(1, id);
                    ps.addBatch();
                }

                int[] results = ps.executeBatch();
                int total = 0;
                for (int r : results) {
                    total += (r >= 0 ? r : 1);
                }
                return total;
            }
        } catch (SQLException e) {
            log.error("Failed to mark records PROCESSING on pipeline '{}': {}", config.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to mark records PROCESSING", e);
        } finally {
            if (connection != null) {
                try {
                    connectionProvider.releaseConnection(connection);
                } catch (SQLException ignored) {}
            }
        }
    }

    @Override
    public int markSent(PipelineConfig config, List<String> recordIds) {
        if (recordIds == null || recordIds.isEmpty()) {
            return 0;
        }

        SqlStatements sql = getOrCreateStatements(config);
        Connection connection = null;
        try {
            connection = connectionProvider.getConnection();
            try (PreparedStatement ps = connection.prepareStatement(sql.updateSent)) {
                for (String id : recordIds) {
                    ps.setString(1, id);
                    ps.addBatch();
                }

                int[] results = ps.executeBatch();
                int total = 0;
                for (int r : results) {
                    total += (r >= 0 ? r : 1);
                }
                return total;
            }
        } catch (SQLException e) {
            log.error("Failed to mark records SENT on pipeline '{}': {}", config.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to mark records SENT", e);
        } finally {
            if (connection != null) {
                try {
                    connectionProvider.releaseConnection(connection);
                } catch (SQLException ignored) {}
            }
        }
    }

    @Override
    public int markSentSingle(PipelineConfig config, String recordId) {
        SqlStatements sql = getOrCreateStatements(config);
        Connection connection = null;
        try {
            connection = connectionProvider.getConnection();
            try (PreparedStatement ps = connection.prepareStatement(sql.updateSent)) {
                ps.setString(1, recordId);
                return ps.executeUpdate();
            }
        } catch (SQLException e) {
            log.error("Failed to mark single record '{}' as SENT: {}", recordId, e.getMessage(), e);
            throw new RuntimeException("Failed to mark record SENT", e);
        } finally {
            if (connection != null) {
                try {
                    connectionProvider.releaseConnection(connection);
                } catch (SQLException ignored) {}
            }
        }
    }

    @Override
    public int markFailed(PipelineConfig config, List<String> recordIds, String errorMessage) {
        if (recordIds == null || recordIds.isEmpty()) {
            return 0;
        }

        SqlStatements sql = getOrCreateStatements(config);
        String truncatedError = truncateError(errorMessage);
        Connection connection = null;

        try {
            connection = connectionProvider.getConnection();
            try (PreparedStatement ps = connection.prepareStatement(sql.updateFailed)) {
                for (String id : recordIds) {
                    ps.setString(1, truncatedError);
                    ps.setString(2, id);
                    ps.addBatch();
                }

                int[] results = ps.executeBatch();
                int total = 0;
                for (int r : results) {
                    total += (r >= 0 ? r : 1);
                }
                return total;
            }
        } catch (SQLException e) {
            log.error("Failed to mark records FAILED on pipeline '{}': {}", config.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to mark records FAILED", e);
        } finally {
            if (connection != null) {
                try {
                    connectionProvider.releaseConnection(connection);
                } catch (SQLException ignored) {}
            }
        }
    }

    @Override
    public int markFailedSingle(PipelineConfig config, String recordId, String errorMessage) {
        SqlStatements sql = getOrCreateStatements(config);
        String truncatedError = truncateError(errorMessage);
        Connection connection = null;

        try {
            connection = connectionProvider.getConnection();
            try (PreparedStatement ps = connection.prepareStatement(sql.updateFailed)) {
                ps.setString(1, truncatedError);
                ps.setString(2, recordId);
                return ps.executeUpdate();
            }
        } catch (SQLException e) {
            log.error("Failed to mark record '{}' as FAILED: {}", recordId, e.getMessage(), e);
            throw new RuntimeException("Failed to mark record FAILED", e);
        } finally {
            if (connection != null) {
                try {
                    connectionProvider.releaseConnection(connection);
                } catch (SQLException ignored) {}
            }
        }
    }

    @Override
    public int reapStaleRecords(PipelineConfig config, int timeoutSeconds, int maxRetries) {
        SqlStatements sql = getOrCreateStatements(config);
        Connection connection = null;
        try {
            connection = connectionProvider.getConnection();
            try (PreparedStatement ps = connection.prepareStatement(sql.reapStale)) {
                return ps.executeUpdate();
            }
        } catch (SQLException e) {
            log.error("Failed to reap stale records on pipeline '{}': {}", config.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to reap stale records", e);
        } finally {
            if (connection != null) {
                try {
                    connectionProvider.releaseConnection(connection);
                } catch (SQLException ignored) {}
            }
        }
    }

    @Override
    public String insertRecord(PipelineConfig config, OutboxRecord record) {
        SqlStatements sql = getOrCreateStatements(config);
        String recordId = record.getOutboxId() != null ? record.getOutboxId() : UUID.randomUUID().toString();
        String headersJson = serializeHeaders(record.getHeaders());
        Connection connection = null;

        try {
            connection = connectionProvider.getConnection();
            try (PreparedStatement ps = connection.prepareStatement(sql.insertRecord)) {
                ps.setString(1, recordId);
                ps.setString(2, record.getTopic());
                ps.setString(3, record.getPartitionKey());
                ps.setString(4, record.getPayload());
                ps.setString(5, headersJson);
                ps.setString(6, record.getStatus() != null ? record.getStatus().name() : OutboxStatus.NEW.name());
                ps.setInt(7, record.getRetryCount());
                ps.setString(8, record.getLastError());

                ps.executeUpdate();
                return recordId;
            }
        } catch (SQLException e) {
            log.error("Failed to insert outbox record into '{}': {}", config.getTableName(), e.getMessage(), e);
            throw new RuntimeException("Failed to insert outbox record", e);
        } finally {
            if (connection != null) {
                try {
                    connectionProvider.releaseConnection(connection);
                } catch (SQLException ignored) {}
            }
        }
    }

    @Override
    public Optional<OutboxRecord> findById(PipelineConfig config, String recordId) {
        SqlStatements sql = getOrCreateStatements(config);
        Connection connection = null;
        try {
            connection = connectionProvider.getConnection();
            try (PreparedStatement ps = connection.prepareStatement(sql.selectById)) {
                ps.setString(1, recordId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return Optional.of(mapRowToRecord(rs, config.getName()));
                    }
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            log.error("Failed to find outbox record by ID '{}': {}", recordId, e.getMessage(), e);
            throw new RuntimeException("Failed to query outbox record", e);
        } finally {
            if (connection != null) {
                try {
                    connectionProvider.releaseConnection(connection);
                } catch (SQLException ignored) {}
            }
        }
    }

    private OutboxRecord mapRowToRecord(ResultSet rs, String pipeline) throws SQLException {
        String id = rs.getString(1);
        String topic = rs.getString(2);
        String partitionKey = rs.getString(3);
        String payload = rs.getString(4);
        String headersJson = rs.getString(5);
        String statusStr = rs.getString(6);
        int retryCount = rs.getInt(7);
        String lastError = rs.getString(8);

        Timestamp createdTs = rs.getTimestamp(9);
        Timestamp updatedTs = rs.getTimestamp(10);
        Timestamp processedTs = rs.getTimestamp(11);

        Instant createdAt = createdTs != null ? createdTs.toInstant() : Instant.now();
        Instant updatedAt = updatedTs != null ? updatedTs.toInstant() : Instant.now();
        Instant processedAt = processedTs != null ? processedTs.toInstant() : null;

        Map<String, String> headers = deserializeHeaders(headersJson);

        return DefaultOutboxRecord.builder()
                .outboxPipeline(pipeline)
                .outboxId(id)
                .topic(topic)
                .partitionKey(partitionKey)
                .payload(payload)
                .headers(headers)
                .status(OutboxStatus.fromString(statusStr))
                .retryCount(retryCount)
                .lastError(lastError)
                .createdAt(createdAt)
                .updatedAt(updatedAt)
                .processedAt(processedAt)
                .build();
    }

    private String serializeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(headers);
        } catch (Exception e) {
            return "{}";
        }
    }

    private Map<String, String> deserializeHeaders(String json) {
        if (json == null || json.trim().isEmpty() || "{}".equals(json.trim())) {
            return Collections.emptyMap();
        }
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    private String truncateError(String error) {
        if (error == null) return null;
        return error.length() > 4000 ? error.substring(0, 4000) : error;
    }

    /**
     * Immutable container of precomputed SQL statements for a pipeline.
     */
    private static final class SqlStatements {
        final String selectBatch;
        final String updateProcessing;
        final String updateSent;
        final String updateFailed;
        final String reapStale;
        final String insertRecord;
        final String selectById;

        private SqlStatements(String selectBatch, String updateProcessing, String updateSent,
                              String updateFailed, String reapStale, String insertRecord, String selectById) {
            this.selectBatch = selectBatch;
            this.updateProcessing = updateProcessing;
            this.updateSent = updateSent;
            this.updateFailed = updateFailed;
            this.reapStale = reapStale;
            this.insertRecord = insertRecord;
            this.selectById = selectById;
        }

        static SqlStatements build(PipelineConfig config, DatabaseDialect dialect) {
            String table = config.getTableName();
            ColumnMapping col = config.getColumns();
            String ts = dialect.getTimestampFunction();

            String selectBatch = dialect.buildSelectBatchForUpdateQuery(
                    table, col, config.getBatchSize(), config.getMaxRetries()
            );

            String updateProcessing = "UPDATE " + table +
                    " SET " + col.getStatus() + " = 'PROCESSING', " +
                    col.getUpdatedAt() + " = " + ts +
                    " WHERE " + col.getId() + " = ?";

            String updateSent = "UPDATE " + table +
                    " SET " + col.getStatus() + " = 'SENT', " +
                    col.getProcessedAt() + " = " + ts + ", " +
                    col.getUpdatedAt() + " = " + ts +
                    " WHERE " + col.getId() + " = ?";

            String updateFailed = "UPDATE " + table +
                    " SET " + col.getStatus() + " = 'FAILED', " +
                    col.getRetryCount() + " = " + col.getRetryCount() + " + 1, " +
                    col.getLastError() + " = ?, " +
                    col.getUpdatedAt() + " = " + ts +
                    " WHERE " + col.getId() + " = ?";

            String reapStale = dialect.buildReapStaleQuery(
                    table, col, config.getProcessingTimeoutSeconds(), config.getMaxRetries()
            );

            String insertRecord = "INSERT INTO " + table + " (" +
                    col.getId() + ", " +
                    col.getTopic() + ", " +
                    col.getPartitionKey() + ", " +
                    col.getPayload() + ", " +
                    col.getHeaders() + ", " +
                    col.getStatus() + ", " +
                    col.getRetryCount() + ", " +
                    col.getLastError() + ", " +
                    col.getCreatedAt() + ", " +
                    col.getUpdatedAt() +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, " + ts + ", " + ts + ")";

            String selectById = "SELECT " +
                    col.getId() + ", " +
                    col.getTopic() + ", " +
                    col.getPartitionKey() + ", " +
                    col.getPayload() + ", " +
                    col.getHeaders() + ", " +
                    col.getStatus() + ", " +
                    col.getRetryCount() + ", " +
                    col.getLastError() + ", " +
                    col.getCreatedAt() + ", " +
                    col.getUpdatedAt() + ", " +
                    col.getProcessedAt() +
                    " FROM " + table +
                    " WHERE " + col.getId() + " = ?";

            return new SqlStatements(selectBatch, updateProcessing, updateSent, updateFailed, reapStale, insertRecord, selectById);
        }
    }
}
