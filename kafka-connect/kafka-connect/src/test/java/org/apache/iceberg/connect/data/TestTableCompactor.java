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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTableCompactor {

  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "id2", Types.LongType.get())),
          ImmutableSet.of(1, 3));

  @TempDir File tempDir;

  private Table table;
  private IcebergSinkConfig config;

  @BeforeEach
  public void before() {
    HadoopTables tables = new HadoopTables();
    table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(),
            tempDir.getAbsolutePath() + "/test_table");

    config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", "parquet"));
  }

  @Test
  public void testCompactSmallFiles() throws IOException {
    // Write multiple small batches to create several small files
    int numBatches = 5;
    int recordsPerBatch = 10;
    int totalRecords = numBatches * recordsPerBatch;

    for (int batch = 0; batch < numBatches; batch++) {
      writeRecords(batch * recordsPerBatch, recordsPerBatch);
    }

    // Verify we have multiple data files
    List<FileScanTask> filesBefore = scanFiles();
    assertThat(filesBefore).hasSize(numBatches);

    // Run compaction with a large target size so all files get merged
    TableCompactor compactor = new TableCompactor(512 * 1024 * 1024, 2, 100);
    TableCompactor.CompactionResult result = compactor.compact(table, null);

    assertThat(result.hasChanges()).isTrue();
    assertThat(result.filesRemoved()).isEqualTo(numBatches);
    assertThat(result.filesAdded()).isEqualTo(1);

    // Verify data is preserved
    List<FileScanTask> filesAfter = scanFiles();
    assertThat(filesAfter).hasSize(1);

    int recordCount = countRecords();
    assertThat(recordCount).isEqualTo(totalRecords);
  }

  @Test
  public void testSkipCompactionBelowThreshold() {
    // Write only 2 small files
    writeRecords(0, 10);
    writeRecords(10, 10);

    // Compactor requires at least 5 small files
    TableCompactor compactor = new TableCompactor(512 * 1024 * 1024, 5, 100);
    TableCompactor.CompactionResult result = compactor.compact(table, null);

    assertThat(result.hasChanges()).isFalse();
  }

  @Test
  public void testNoCompactionWhenNoSmallFiles() {
    // Write a single file — it's small but below min threshold of 2
    writeRecords(0, 10);

    TableCompactor compactor = new TableCompactor(512 * 1024 * 1024, 2, 100);
    TableCompactor.CompactionResult result = compactor.compact(table, null);

    assertThat(result.hasChanges()).isFalse();
  }

  @Test
  public void testMaxFilesPerCompaction() throws IOException {
    // Write 10 small files
    for (int i = 0; i < 10; i++) {
      writeRecords(i * 5, 5);
    }

    // Limit compaction to 5 files at a time
    TableCompactor compactor = new TableCompactor(512 * 1024 * 1024, 2, 5);
    TableCompactor.CompactionResult result = compactor.compact(table, null);

    assertThat(result.hasChanges()).isTrue();
    // Should have compacted at most 5 files
    assertThat(result.filesRemoved()).isLessThanOrEqualTo(5);

    // Total records should be preserved
    int recordCount = countRecords();
    assertThat(recordCount).isEqualTo(50);
  }

  @Test
  public void testCompactionPreservesData() throws IOException {
    // Write specific records and verify they survive compaction
    List<Record> expectedRecords = Lists.newArrayList();
    for (int batch = 0; batch < 3; batch++) {
      List<Record> batchRecords = createRecords(batch * 100, 100);
      expectedRecords.addAll(batchRecords);
      writeRecordList(batchRecords);
    }

    TableCompactor compactor = new TableCompactor(512 * 1024 * 1024, 2, 100);
    compactor.compact(table, null);

    // Read back all records and verify count
    int recordCount = countRecords();
    assertThat(recordCount).isEqualTo(300);
  }

  @Test
  public void testFindSmallFiles() {
    // Write 3 small files
    writeRecords(0, 10);
    writeRecords(10, 10);
    writeRecords(20, 10);

    // These files should all be well under 128MB
    TableCompactor compactor = new TableCompactor(128 * 1024 * 1024, 2, 100);
    List<FileScanTask> smallFiles = compactor.findSmallFiles(table);
    assertThat(smallFiles).hasSize(3);

    // With a tiny target size, no files should be considered "small"
    TableCompactor tinyCompactor = new TableCompactor(1, 2, 100);
    List<FileScanTask> noSmallFiles = tinyCompactor.findSmallFiles(table);
    assertThat(noSmallFiles).isEmpty();
  }

  private void writeRecords(int startId, int count) {
    List<Record> records = createRecords(startId, count);
    writeRecordList(records);
  }

  private void writeRecordList(List<Record> records) {
    TableReference tableRef =
        TableReference.of("test_catalog", TableIdentifier.of("test_table"), UUID.randomUUID());
    try (TaskWriter<Record> writer =
        RecordUtils.createTableWriter(table, tableRef, config)) {
      for (Record record : records) {
        writer.write(record);
      }
      WriteResult result = writer.complete();

      // Commit the files to the table
      if (result.dataFiles().length > 0) {
        org.apache.iceberg.AppendFiles append = table.newAppend();
        for (DataFile dataFile : result.dataFiles()) {
          append.appendFile(dataFile);
        }
        append.commit();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<Record> createRecords(int startId, int count) {
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      Record record = GenericRecord.create(SCHEMA);
      record.setField("id", (long) (startId + i));
      record.setField("data", "record-" + (startId + i));
      record.setField("id2", (long) ((startId + i) % 100));
      records.add(record);
    }
    return records;
  }

  private List<FileScanTask> scanFiles() throws IOException {
    List<FileScanTask> files = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(files::add);
    }
    return files;
  }

  private int countRecords() throws IOException {
    int count = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record ignored : records) {
        count++;
      }
    }
    return count;
  }
}