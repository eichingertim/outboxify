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

export declare const OutboxStatus: {
  readonly NEW: 'NEW';
  readonly PROCESSING: 'PROCESSING';
  readonly SENT: 'SENT';
  readonly FAILED: 'FAILED';
};

export type OutboxStatusType = typeof OutboxStatus[keyof typeof OutboxStatus];

export declare function isEligibleForProcessing(status: string): boolean;

export declare class OutboxPayload {
  readonly topic: string;
  readonly partitionKey?: string | null;
  readonly payload: string;
  readonly headers: Record<string, string>;

  static of(topic: string, payload: unknown, partitionKey?: string | null, headers?: Record<string, string>): OutboxPayload;
  static builder(): OutboxPayloadBuilder;

  getTopic(): string;
  getPartitionKey(): string | null | undefined;
  getPayload(): string;
  getHeaders(): Record<string, string>;
}

export declare class OutboxPayloadBuilder {
  topic(topic: string): this;
  partitionKey(key: string): this;
  payload(payload: unknown): this;
  headers(headers: Record<string, string>): this;
  header(key: string, value: string): this;
  build(): OutboxPayload;
}

export interface OutboxRecord {
  getOutboxPipeline(): string;
  getOutboxId(): string;
  getTopic(): string;
  getPartitionKey?(): string | null;
  getPayload(): string;
  getHeaders?(): Record<string, string>;
  getStatus?(): OutboxStatusType;
  getRetryCount?(): number;
  getLastError?(): string | null;
  getCreatedAt?(): Date;
  getUpdatedAt?(): Date;
  getProcessedAt?(): Date | null;
}

export declare class DefaultOutboxRecord implements OutboxRecord {
  outboxPipeline: string;
  outboxId: string;
  topic: string;
  partitionKey?: string | null;
  payload: string;
  headers: Record<string, string>;
  status: OutboxStatusType;
  retryCount: number;
  lastError?: string | null;
  createdAt: Date;
  updatedAt: Date;
  processedAt?: Date | null;

  constructor(options: {
    outboxPipeline: string;
    outboxId: string;
    topic: string;
    partitionKey?: string | null;
    payload: string;
    headers?: Record<string, string>;
    status?: OutboxStatusType;
    retryCount?: number;
    lastError?: string | null;
    createdAt?: Date;
    updatedAt?: Date;
    processedAt?: Date | null;
  });

  static fromPayload(pipeline: string, id: string, payload: OutboxPayload): DefaultOutboxRecord;

  getOutboxPipeline(): string;
  getOutboxId(): string;
  getTopic(): string;
  getPartitionKey(): string | null | undefined;
  getPayload(): string;
  getHeaders(): Record<string, string>;
  getStatus(): OutboxStatusType;
  getRetryCount(): number;
  getLastError(): string | null | undefined;
  getCreatedAt(): Date;
  getUpdatedAt(): Date;
  getProcessedAt(): Date | null | undefined;
}

export declare class OutboxResult {
  readonly recordId: string;
  readonly status: OutboxStatusType;
  readonly error?: Error | null;

  constructor(recordId: string, status: OutboxStatusType, error?: Error | null);
  static success(recordId: string): OutboxResult;
  static failure(recordId: string, error: Error): OutboxResult;
}

export declare class ColumnMapping {
  readonly id: string;
  readonly topic: string;
  readonly partitionKey: string;
  readonly payload: string;
  readonly headers: string;
  readonly status: string;
  readonly retryCount: string;
  readonly lastError: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly processedAt: string;

  constructor(options?: Partial<ColumnMapping>);
}

export declare class PipelineConfig {
  readonly name: string;
  readonly tableName: string;
  readonly dialect: string;
  readonly batchSize: number;
  readonly pollIntervalMs: number;
  readonly processingTimeoutSeconds: number;
  readonly reaperIntervalMs: number;
  readonly maxRetries: number;
  readonly columns: ColumnMapping;

  constructor(options: {
    name: string;
    tableName: string;
    dialect?: string;
    batchSize?: number;
    pollIntervalMs?: number;
    processingTimeoutSeconds?: number;
    reaperIntervalMs?: number;
    maxRetries?: number;
    columns?: Partial<ColumnMapping>;
  });
}

export declare const DialectType: {
  readonly ORACLE: 'ORACLE';
  readonly POSTGRESQL: 'POSTGRESQL';
  readonly MYSQL: 'MYSQL';
  readonly MSSQL: 'MSSQL';
  readonly SQLITE: 'SQLITE';
  readonly AUTO_DETECT: 'AUTO_DETECT';
};

export declare abstract class DatabaseDialect {
  abstract getName(): string;
  abstract buildSelectBatchForUpdateQuery(table: string, cols: ColumnMapping, batchSize: number, maxRetries: number): string;
  abstract buildMarkBatchProcessingQuery(table: string, cols: ColumnMapping, idsCount: number): string;
  abstract buildMarkSentQuery(table: string, cols: ColumnMapping): string;
  abstract buildMarkFailedQuery(table: string, cols: ColumnMapping): string;
  abstract buildReapStaleQuery(table: string, cols: ColumnMapping, timeoutSeconds: number, maxRetries: number): string;
  abstract getSparseIndexDdl(table: string, indexName: string, cols: ColumnMapping): string;
}

export declare class OracleDialect extends DatabaseDialect {
  getName(): string;
  buildSelectBatchForUpdateQuery(table: string, cols: ColumnMapping, batchSize: number, maxRetries: number): string;
  buildMarkBatchProcessingQuery(table: string, cols: ColumnMapping, idsCount: number): string;
  buildMarkSentQuery(table: string, cols: ColumnMapping): string;
  buildMarkFailedQuery(table: string, cols: ColumnMapping): string;
  buildReapStaleQuery(table: string, cols: ColumnMapping, timeoutSeconds: number, maxRetries: number): string;
  getSparseIndexDdl(table: string, indexName: string, cols: ColumnMapping): string;
}

export declare class PostgresDialect extends DatabaseDialect {
  getName(): string;
  buildSelectBatchForUpdateQuery(table: string, cols: ColumnMapping, batchSize: number, maxRetries: number): string;
  buildMarkBatchProcessingQuery(table: string, cols: ColumnMapping, idsCount: number): string;
  buildMarkSentQuery(table: string, cols: ColumnMapping): string;
  buildMarkFailedQuery(table: string, cols: ColumnMapping): string;
  buildReapStaleQuery(table: string, cols: ColumnMapping, timeoutSeconds: number, maxRetries: number): string;
  getSparseIndexDdl(table: string, indexName: string, cols: ColumnMapping): string;
}

export declare class MySqlDialect extends DatabaseDialect {
  getName(): string;
  buildSelectBatchForUpdateQuery(table: string, cols: ColumnMapping, batchSize: number, maxRetries: number): string;
  buildMarkBatchProcessingQuery(table: string, cols: ColumnMapping, idsCount: number): string;
  buildMarkSentQuery(table: string, cols: ColumnMapping): string;
  buildMarkFailedQuery(table: string, cols: ColumnMapping): string;
  buildReapStaleQuery(table: string, cols: ColumnMapping, timeoutSeconds: number, maxRetries: number): string;
  getSparseIndexDdl(table: string, indexName: string, cols: ColumnMapping): string;
}

export declare class SqlServerDialect extends DatabaseDialect {
  getName(): string;
  buildSelectBatchForUpdateQuery(table: string, cols: ColumnMapping, batchSize: number, maxRetries: number): string;
  buildMarkBatchProcessingQuery(table: string, cols: ColumnMapping, idsCount: number): string;
  buildMarkSentQuery(table: string, cols: ColumnMapping): string;
  buildMarkFailedQuery(table: string, cols: ColumnMapping): string;
  buildReapStaleQuery(table: string, cols: ColumnMapping, timeoutSeconds: number, maxRetries: number): string;
  getSparseIndexDdl(table: string, indexName: string, cols: ColumnMapping): string;
}

export declare class SqliteDialect extends DatabaseDialect {
  getName(): string;
  buildSelectBatchForUpdateQuery(table: string, cols: ColumnMapping, batchSize: number, maxRetries: number): string;
  buildMarkBatchProcessingQuery(table: string, cols: ColumnMapping, idsCount: number): string;
  buildMarkSentQuery(table: string, cols: ColumnMapping): string;
  buildMarkFailedQuery(table: string, cols: ColumnMapping): string;
  buildReapStaleQuery(table: string, cols: ColumnMapping, timeoutSeconds: number, maxRetries: number): string;
  getSparseIndexDdl(table: string, indexName: string, cols: ColumnMapping): string;
}

export declare class DialectRegistry {
  register(name: string, dialect: DatabaseDialect): void;
  getDialect(name: string): DatabaseDialect;
}

export interface BrokerPublisher {
  publish(record: OutboxRecord): Promise<void>;
  publishBatch?(records: OutboxRecord[]): Promise<void>;
}

export declare class MockBrokerPublisher implements BrokerPublisher {
  readonly publishedRecords: OutboxRecord[];
  publish(record: OutboxRecord): Promise<void>;
  publishBatch(records: OutboxRecord[]): Promise<void>;
  clear(): void;
}

export declare class KafkaBrokerPublisher implements BrokerPublisher {
  constructor(options: { bootstrapServers: string; [key: string]: unknown });
  publish(record: OutboxRecord): Promise<void>;
  publishBatch(records: OutboxRecord[]): Promise<void>;
}

export interface OutboxRepository {
  insertRecord(config: PipelineConfig, record: OutboxRecord, tx?: unknown): Promise<string>;
  claimBatch(config: PipelineConfig): Promise<OutboxRecord[]>;
  markSent(config: PipelineConfig, recordId: string): Promise<void>;
  markFailed(config: PipelineConfig, recordId: string, error: string): Promise<void>;
  reapStaleRecords(config: PipelineConfig): Promise<number>;
  findById(config: PipelineConfig, recordId: string): Promise<OutboxRecord | null>;
}

export declare class InMemoryOutboxRepository implements OutboxRepository {
  readonly records: Map<string, OutboxRecord>;
  insertRecord(config: PipelineConfig, record: OutboxRecord, tx?: unknown): Promise<string>;
  claimBatch(config: PipelineConfig): Promise<OutboxRecord[]>;
  markSent(config: PipelineConfig, recordId: string): Promise<void>;
  markFailed(config: PipelineConfig, recordId: string, error: string): Promise<void>;
  reapStaleRecords(config: PipelineConfig): Promise<number>;
  findById(config: PipelineConfig, recordId: string): Promise<OutboxRecord | null>;
}

export declare class OutboxDispatcher {
  constructor(repository: OutboxRepository, brokerPublisher: BrokerPublisher);
  dispatchRecord(config: PipelineConfig, record: OutboxRecord): Promise<OutboxResult>;
  dispatchBatch(config: PipelineConfig, records: OutboxRecord[]): Promise<OutboxResult[]>;
}

export declare class OutboxPollerEngine {
  readonly running: boolean;
  constructor(config: PipelineConfig, repository: OutboxRepository, dispatcher: OutboxDispatcher);
  start(): void;
  stop(): Promise<void>;
  pollOnce(): Promise<number>;
}

export declare class OutboxReaperJob {
  readonly running: boolean;
  constructor(config: PipelineConfig, repository: OutboxRepository);
  start(): void;
  stop(): Promise<void>;
  reapOnce(): Promise<number>;
}

export declare class OutboxHook {
  constructor(dispatcher: OutboxDispatcher, configResolver: (name: string) => PipelineConfig | null);
  registerForCommit(pipeline: string, recordId: string, payload: OutboxPayload): void;
  triggerCommit(pipeline: string, recordId: string, payload: OutboxPayload): Promise<void>;
}

export declare class OutboxPublisher {
  constructor(
    repository: OutboxRepository,
    hook: OutboxHook,
    dispatcher: OutboxDispatcher,
    configResolver: (name: string) => PipelineConfig | null
  );
  stage(pipeline: string, payload: OutboxPayload, tx?: unknown): Promise<string>;
  publish(pipeline: string, payload: OutboxPayload, tx?: unknown): Promise<OutboxResult>;
}
