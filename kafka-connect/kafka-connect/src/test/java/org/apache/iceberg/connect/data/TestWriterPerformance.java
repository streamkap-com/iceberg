/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("performance")
public class TestWriterPerformance extends WriterTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestWriterPerformance.class);

  private static final int WARMUP_RECORDS = 1_000;
  private static final int BENCHMARK_RECORDS = 100_000;
  private static final int WARMUP_ITERATIONS = 2;
  private static final int BENCHMARK_ITERATIONS = 3;

  // Wider schema for realistic payloads
  private static final Schema WIDE_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "id2", Types.LongType.get()),
              Types.NestedField.optional(4, "name", Types.StringType.get()),
              Types.NestedField.optional(5, "email", Types.StringType.get()),
              Types.NestedField.optional(6, "amount", Types.DoubleType.get()),
              Types.NestedField.optional(7, "status", Types.StringType.get()),
              Types.NestedField.optional(8, "created_at", Types.LongType.get()),
              Types.NestedField.optional(9, "updated_at", Types.LongType.get()),
              Types.NestedField.optional(10, "__deleted", Types.BooleanType.get())),
          ImmutableSet.of(1, 3));

  private static final PartitionSpec WIDE_SPEC =
      PartitionSpec.builderFor(WIDE_SCHEMA).identity("status").build();

  // --- Append-only mode ---

  @Test
  public void testAppendUnpartitionedThroughput() {
    when(table.schema()).thenReturn(WIDE_SCHEMA);
    IcebergSinkConfig config = appendConfig();

    BenchmarkResult result =
        runBenchmark("Append Unpartitioned", config, UnpartitionedWriter.class, false, false);

    LOG.info("{}", result);
    assertThat(result.avgRecordsPerSec).isGreaterThan(10_000);
  }

  @Test
  public void testAppendPartitionedThroughput() {
    when(table.schema()).thenReturn(WIDE_SCHEMA);
    when(table.spec()).thenReturn(WIDE_SPEC);
    IcebergSinkConfig config = appendConfig();

    BenchmarkResult result =
        runBenchmark("Append Partitioned", config, PartitionedAppendWriter.class, false, false);

    LOG.info("{}", result);
    assertThat(result.avgRecordsPerSec).isGreaterThan(5_000);
  }

  // --- CDC mode ---

  @Test
  public void testCdcInsertThroughput() {
    when(table.schema()).thenReturn(WIDE_SCHEMA);
    IcebergSinkConfig config = cdcConfig();

    BenchmarkResult result =
        runBenchmark("CDC Insert Unpartitioned", config, UnpartitionedDeltaWriter.class, false, false);

    LOG.info("{}", result);
    assertThat(result.avgRecordsPerSec).isGreaterThan(10_000);
  }

  @Test
  public void testCdcMixedOpsThroughput() {
    when(table.schema()).thenReturn(WIDE_SCHEMA);
    IcebergSinkConfig config = cdcConfig();

    BenchmarkResult result =
        runBenchmark(
            "CDC Mixed Ops Unpartitioned", config, UnpartitionedDeltaWriter.class, true, false);

    LOG.info("{}", result);
    assertThat(result.avgRecordsPerSec).isGreaterThan(5_000);
  }

  @Test
  public void testCdcPartitionedMixedOpsThroughput() {
    when(table.schema()).thenReturn(WIDE_SCHEMA);
    when(table.spec()).thenReturn(WIDE_SPEC);
    IcebergSinkConfig config = cdcConfig();

    BenchmarkResult result =
        runBenchmark(
            "CDC Mixed Ops Partitioned", config, PartitionedDeltaWriter.class, true, false);

    LOG.info("{}", result);
    assertThat(result.avgRecordsPerSec).isGreaterThan(3_000);
  }

  // --- Upsert mode ---

  @Test
  public void testUpsertThroughput() {
    when(table.schema()).thenReturn(WIDE_SCHEMA);
    IcebergSinkConfig config = upsertConfig();

    BenchmarkResult result =
        runBenchmark("Upsert Unpartitioned", config, UnpartitionedDeltaWriter.class, false, false);

    LOG.info("{}", result);
    assertThat(result.avgRecordsPerSec).isGreaterThan(5_000);
  }

  // --- Hard delete mode ---

  @Test
  public void testHardDeleteThroughput() {
    when(table.schema()).thenReturn(WIDE_SCHEMA);
    IcebergSinkConfig config = hardDeleteConfig();

    BenchmarkResult result =
        runBenchmark(
            "Hard Delete Unpartitioned", config, UnpartitionedDeltaWriter.class, false, true);

    LOG.info("{}", result);
    assertThat(result.avgRecordsPerSec).isGreaterThan(5_000);
  }

  // --- Batch size latency ---

  @Test
  public void testBatchLatencyScaling() {
    when(table.schema()).thenReturn(WIDE_SCHEMA);
    IcebergSinkConfig config = appendConfig();

    int[] batchSizes = {100, 1_000, 10_000, 50_000};
    for (int batchSize : batchSizes) {
      List<Record> rows = generateRows(batchSize, false, false);
      long start = System.nanoTime();
      writeAndComplete(rows, config);
      long elapsed = System.nanoTime() - start;
      double latencyMs = elapsed / 1_000_000.0;
      double perRecordUs = (elapsed / 1_000.0) / batchSize;

      LOG.info(
          "Batch size {}: latency={}ms, per-record={}us",
          batchSize,
          String.format("%.1f", latencyMs),
          String.format("%.1f", perRecordUs));

      // Batch of 50k should complete under 10 seconds
      assertThat(latencyMs).isLessThan(10_000);
    }
  }

  // --- Benchmark infrastructure ---

  private BenchmarkResult runBenchmark(
      String name,
      IcebergSinkConfig config,
      Class<?> writerClass,
      boolean mixedOps,
      boolean hardDelete) {

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      List<Record> warmup = generateRows(WARMUP_RECORDS, mixedOps, hardDelete);
      writeAndComplete(warmup, config);
    }

    // Benchmark
    long totalNanos = 0;
    long totalRecords = 0;
    long minNanos = Long.MAX_VALUE;
    long maxNanos = Long.MIN_VALUE;
    int totalDataFiles = 0;
    int totalDeleteFiles = 0;

    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      List<Record> rows = generateRows(BENCHMARK_RECORDS, mixedOps, hardDelete);

      long start = System.nanoTime();
      WriteResult result = writeAndComplete(rows, config);
      long elapsed = System.nanoTime() - start;

      totalNanos += elapsed;
      totalRecords += BENCHMARK_RECORDS;
      minNanos = Math.min(minNanos, elapsed);
      maxNanos = Math.max(maxNanos, elapsed);
      totalDataFiles += result.dataFiles().length;
      totalDeleteFiles += result.deleteFiles().length;
    }

    return new BenchmarkResult(
        name,
        BENCHMARK_RECORDS,
        BENCHMARK_ITERATIONS,
        totalNanos,
        totalRecords,
        minNanos,
        maxNanos,
        totalDataFiles,
        totalDeleteFiles);
  }

  private WriteResult writeAndComplete(List<Record> rows, IcebergSinkConfig config) {
    TableReference tableReference =
        TableReference.of("test_catalog", TableIdentifier.of("name"), UUID.randomUUID());
    try (TaskWriter<Record> writer =
        RecordUtils.createTableWriter(table, tableReference, config)) {
      for (Record row : rows) {
        writer.write(row);
      }
      return writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private List<Record> generateRows(int count, boolean mixedOps, boolean hardDelete) {
    String[] statuses = {"active", "pending", "completed", "cancelled"};
    List<Record> rows = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      Record row = GenericRecord.create(WIDE_SCHEMA);
      row.setField("id", (long) i);
      row.setField("data", "record-" + i);
      row.setField("id2", (long) (i % 1000));
      row.setField("name", "user-" + (i % 500));
      row.setField("email", "user" + (i % 500) + "@example.com");
      row.setField("amount", (double) i * 1.5);
      row.setField("status", statuses[i % statuses.length]);
      row.setField("created_at", System.currentTimeMillis());
      row.setField("updated_at", System.currentTimeMillis());
      row.setField("__deleted", false);

      if (mixedOps) {
        // 60% INSERT, 25% UPDATE, 15% DELETE
        int mod = i % 20;
        if (mod < 12) {
          // INSERT — no wrapper needed, default behavior
        } else if (mod < 17) {
          row = new RecordWrapper(row, Operation.UPDATE);
        } else {
          row = new RecordWrapper(row, Operation.DELETE);
        }
      }

      if (hardDelete && i % 10 == 0) {
        // 10% of records are hard deletes
        row.setField("__deleted", true);
      }

      rows.add(row);
    }

    return rows;
  }

  // --- Config helpers ---

  private IcebergSinkConfig appendConfig() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", "parquet"));
    return config;
  }

  private IcebergSinkConfig cdcConfig() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", "parquet"));
    when(config.tablesCdcField()).thenReturn("__op");
    return config;
  }

  private IcebergSinkConfig upsertConfig() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", "parquet"));
    when(config.upsertModeEnabled()).thenReturn(true);
    return config;
  }

  private IcebergSinkConfig hardDeleteConfig() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", "parquet"));
    when(config.hardDeleteEnabled()).thenReturn(true);
    when(config.hardDeleteField()).thenReturn("__deleted");
    return config;
  }

  // --- Result reporting ---

  private static class BenchmarkResult {
    final String name;
    final int recordsPerIteration;
    final int iterations;
    final long totalNanos;
    final long totalRecords;
    final long minNanos;
    final long maxNanos;
    final double avgRecordsPerSec;
    final double avgLatencyMs;
    final int totalDataFiles;
    final int totalDeleteFiles;

    BenchmarkResult(
        String name,
        int recordsPerIteration,
        int iterations,
        long totalNanos,
        long totalRecords,
        long minNanos,
        long maxNanos,
        int totalDataFiles,
        int totalDeleteFiles) {
      this.name = name;
      this.recordsPerIteration = recordsPerIteration;
      this.iterations = iterations;
      this.totalNanos = totalNanos;
      this.totalRecords = totalRecords;
      this.minNanos = minNanos;
      this.maxNanos = maxNanos;
      this.totalDataFiles = totalDataFiles;
      this.totalDeleteFiles = totalDeleteFiles;
      this.avgRecordsPerSec = (totalRecords * 1_000_000_000.0) / totalNanos;
      this.avgLatencyMs = (totalNanos / 1_000_000.0) / iterations;
    }

    @Override
    public String toString() {
      double minMs = minNanos / 1_000_000.0;
      double maxMs = maxNanos / 1_000_000.0;
      double perRecordUs = (totalNanos / 1_000.0) / totalRecords;

      return String.format(
          "\n=== %s ===\n"
              + "  Records per iteration : %,d\n"
              + "  Iterations            : %d\n"
              + "  Avg throughput        : %,.0f records/sec\n"
              + "  Avg batch latency     : %.1f ms\n"
              + "  Min batch latency     : %.1f ms\n"
              + "  Max batch latency     : %.1f ms\n"
              + "  Avg per-record latency: %.2f us\n"
              + "  Data files created    : %d\n"
              + "  Delete files created  : %d\n",
          name,
          recordsPerIteration,
          iterations,
          avgRecordsPerSec,
          avgLatencyMs,
          minMs,
          maxMs,
          perRecordUs,
          totalDataFiles,
          totalDeleteFiles);
    }
  }
}
