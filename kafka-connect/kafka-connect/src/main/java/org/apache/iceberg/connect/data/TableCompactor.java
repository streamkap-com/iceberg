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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight in-connector compaction that merges small data files into larger ones using the
 * Iceberg read/write APIs and atomic {@link RewriteFiles} commit.
 */
public class TableCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(TableCompactor.class);

  private final long targetFileSizeBytes;
  private final int minSmallFiles;
  private final int maxFilesPerCompaction;
  private final boolean expireAfterCompaction;
  private final int retainLastSnapshots;

  public TableCompactor(
      long targetFileSizeBytes,
      int minSmallFiles,
      int maxFilesPerCompaction,
      boolean expireAfterCompaction,
      int retainLastSnapshots) {
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.minSmallFiles = minSmallFiles;
    this.maxFilesPerCompaction = maxFilesPerCompaction;
    this.expireAfterCompaction = expireAfterCompaction;
    this.retainLastSnapshots = retainLastSnapshots;
  }

  public TableCompactor(
      long targetFileSizeBytes, int minSmallFiles, int maxFilesPerCompaction) {
    this(targetFileSizeBytes, minSmallFiles, maxFilesPerCompaction, false, 1);
  }

  /**
   * Compact small data files in the given table. Files smaller than {@code targetFileSizeBytes} are
   * grouped by partition and rewritten into larger files.
   *
   * @param table the Iceberg table to compact
   * @param branch optional branch name (null for main)
   * @return the result of the compaction
   */
  public CompactionResult compact(Table table, String branch) {
    table.refresh();

    List<FileScanTask> smallFiles = findSmallFiles(table);
    if (smallFiles.size() < minSmallFiles) {
      LOG.debug(
          "Table {} has {} small files, below threshold of {}. Skipping compaction.",
          table.name(),
          smallFiles.size(),
          minSmallFiles);
      return CompactionResult.EMPTY;
    }

    // Limit the number of files per compaction to bound memory/time
    if (smallFiles.size() > maxFilesPerCompaction) {
      smallFiles = smallFiles.subList(0, maxFilesPerCompaction);
    }

    LOG.info(
        "Starting compaction for table {}: {} small files to merge (target size: {} bytes)",
        table.name(),
        smallFiles.size(),
        targetFileSizeBytes);

    // Group files by partition for correct rewriting
    Map<StructLikeWrapper, List<FileScanTask>> byPartition =
        groupByPartition(table, smallFiles);

    List<DataFile> filesToDelete = new ArrayList<>();
    List<DataFile> filesToAdd = new ArrayList<>();

    for (Map.Entry<StructLikeWrapper, List<FileScanTask>> entry : byPartition.entrySet()) {
      List<FileScanTask> partitionFiles = entry.getValue();

      if (partitionFiles.size() < 2) {
        continue;
      }

      List<DataFile> oldFiles =
          partitionFiles.stream().map(FileScanTask::file).collect(Collectors.toList());

      List<DataFile> newFiles = rewriteFiles(table, partitionFiles);

      filesToDelete.addAll(oldFiles);
      filesToAdd.addAll(newFiles);
    }

    if (filesToDelete.isEmpty()) {
      LOG.info("No files to compact for table {}", table.name());
      return CompactionResult.EMPTY;
    }

    // Atomic swap: delete old small files, add new compacted files
    long snapshotId = table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : -1;

    RewriteFiles rewrite = table.newRewrite();
    if (branch != null) {
      rewrite.toBranch(branch);
    }
    if (snapshotId > 0) {
      rewrite.validateFromSnapshot(snapshotId);
    }

    filesToDelete.forEach(rewrite::deleteFile);
    filesToAdd.forEach(rewrite::addFile);
    rewrite.commit();

    LOG.info(
        "Compaction complete for table {}: merged {} files into {} files",
        table.name(),
        filesToDelete.size(),
        filesToAdd.size());

    int expiredSnapshots = 0;
    if (expireAfterCompaction) {
      try {
        expiredSnapshots = expireSnapshots(table);
      } catch (Exception e) {
        LOG.warn(
            "Snapshot expiration failed for table {} (compaction itself succeeded). "
                + "Old snapshots will be retried on next compaction cycle.",
            table.name(),
            e);
      }
    }

    return new CompactionResult(filesToDelete.size(), filesToAdd.size(), expiredSnapshots);
  }

  List<FileScanTask> findSmallFiles(Table table) {
    List<FileScanTask> smallFiles = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        if (task.file().fileSizeInBytes() < targetFileSizeBytes) {
          smallFiles.add(task);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to scan files for compaction", e);
    }
    return smallFiles;
  }

  private Map<StructLikeWrapper, List<FileScanTask>> groupByPartition(
      Table table, List<FileScanTask> files) {
    StructLikeWrapper template = StructLikeWrapper.forType(table.spec().partitionType());
    Map<StructLikeWrapper, List<FileScanTask>> grouped = Maps.newHashMap();
    for (FileScanTask task : files) {
      StructLikeWrapper key = template.copyFor(task.file().partition());
      grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(task);
    }
    return grouped;
  }

  private List<DataFile> rewriteFiles(Table table, List<FileScanTask> fileScanTasks) {
    Map<String, String> tableProps = Maps.newHashMap(table.properties());
    String formatStr =
        tableProps.getOrDefault(
            TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.fromString(formatStr);

    FileWriterFactory<Record> writerFactory =
        new GenericFileWriterFactory.Builder(table)
            .dataSchema(table.schema())
            .dataFileFormat(format)
            .writerProperties(tableProps)
            .build();

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(format)
            .build();

    TaskWriter<Record> writer;
    if (table.spec().isUnpartitioned()) {
      writer =
          new UnpartitionedWriter<>(
              table.spec(), format, writerFactory, fileFactory, table.io(), targetFileSizeBytes);
    } else {
      writer =
          new PartitionedAppendWriter(
              table.spec(),
              format,
              writerFactory,
              fileFactory,
              table.io(),
              targetFileSizeBytes,
              table.schema());
    }

    try {
      long skippedRecords = 0;
      for (FileScanTask task : fileScanTasks) {
        // Build delete set per-task: task.deletes() returns only the delete files
        // applicable to this specific data file (based on sequence number).
        // This ensures we don't filter out records from newer data files
        // that were written alongside or after the equality delete file.
        Set<List<Object>> taskDeleteKeys =
            buildEqualityDeleteSet(table, List.of(task));
        List<String> taskEqFieldNames =
            getEqualityFieldNames(table, List.of(task));
        boolean taskHasDeletes = !taskDeleteKeys.isEmpty();

        try (CloseableIterable<Record> records = openFile(table, task)) {
          for (Record record : records) {
            if (taskHasDeletes
                && isDeletedRecord(record, taskDeleteKeys, taskEqFieldNames)) {
              skippedRecords++;
              continue;
            }
            writer.write(record);
          }
        }
      }

      if (skippedRecords > 0) {
        LOG.info(
            "Filtered out {} logically-deleted records during compaction for table {}",
            skippedRecords,
            table.name());
      }

      WriteResult result = writer.complete();
      List<DataFile> newFiles = Lists.newArrayList(result.dataFiles());

      LOG.debug(
          "Rewrote {} files into {} files for table {}",
          fileScanTasks.size(),
          newFiles.size(),
          table.name());

      return newFiles;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to rewrite files during compaction", e);
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        LOG.warn("Failed to close writer during compaction", e);
      }
    }
  }

  /**
   * Reads all equality delete files associated with the given scan tasks and builds a set of
   * deleted key tuples. Each key is a list of field values matching the equality field IDs.
   */
  private Set<List<Object>> buildEqualityDeleteSet(
      Table table, List<FileScanTask> fileScanTasks) {
    Set<List<Object>> deleteKeys = new HashSet<>();
    Set<String> processedLocations = new HashSet<>();

    for (FileScanTask task : fileScanTasks) {
      for (DeleteFile deleteFile : task.deletes()) {
        if (deleteFile.content() != FileContent.EQUALITY_DELETES) {
          continue;
        }
        if (!processedLocations.add(deleteFile.location())) {
          continue;
        }

        List<Integer> eqFieldIds = deleteFile.equalityFieldIds();
        List<String> fieldNames =
            eqFieldIds.stream()
                .map(id -> table.schema().findField(id).name())
                .collect(Collectors.toList());

        Schema eqSchema = table.schema().select(fieldNames);

        InputFile input =
            table.io().newInputFile(deleteFile.location(), deleteFile.fileSizeInBytes());
        ReadBuilder<Record, ?> readBuilder =
            FormatModelRegistry.readBuilder(deleteFile.format(), Record.class, input);

        try (CloseableIterable<Record> records =
            readBuilder.project(eqSchema).caseSensitive(true).build()) {
          for (Record record : records) {
            List<Object> key = new ArrayList<>(fieldNames.size());
            for (String fieldName : fieldNames) {
              key.add(record.getField(fieldName));
            }
            deleteKeys.add(key);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(
              "Failed to read equality delete file: " + deleteFile.location(), e);
        }
      }
    }

    return deleteKeys;
  }

  /**
   * Extracts the equality field names from the first equality delete file found in the scan tasks.
   */
  private List<String> getEqualityFieldNames(Table table, List<FileScanTask> fileScanTasks) {
    for (FileScanTask task : fileScanTasks) {
      for (DeleteFile deleteFile : task.deletes()) {
        if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
          return deleteFile.equalityFieldIds().stream()
              .map(id -> table.schema().findField(id).name())
              .collect(Collectors.toList());
        }
      }
    }
    return List.of();
  }

  private boolean isDeletedRecord(
      Record record, Set<List<Object>> deleteKeys, List<String> eqFieldNames) {
    List<Object> key = new ArrayList<>(eqFieldNames.size());
    for (String fieldName : eqFieldNames) {
      key.add(record.getField(fieldName));
    }
    return deleteKeys.contains(key);
  }

  /**
   * Expire old snapshots, retaining the most recent ones. This physically deletes data files that
   * are no longer referenced by any retained snapshot.
   */
  int expireSnapshots(Table table) {
    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot == null) {
      return 0;
    }

    long expireTimestamp = System.currentTimeMillis();

    ExpireSnapshots expire =
        table
            .expireSnapshots()
            .expireOlderThan(expireTimestamp)
            .retainLast(retainLastSnapshots);

    List<Snapshot> expired = expire.apply();
    if (expired.isEmpty()) {
      LOG.debug("No snapshots to expire for table {}", table.name());
      return 0;
    }

    expire.commit();

    LOG.info(
        "Expired {} snapshot(s) for table {}, retained last {}",
        expired.size(),
        table.name(),
        retainLastSnapshots);

    return expired.size();
  }

  private CloseableIterable<Record> openFile(Table table, FileScanTask task) {
    InputFile input = table.io().newInputFile(task.file());
    Map<Integer, ?> partition =
        PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);

    ReadBuilder<Record, ?> builder =
        FormatModelRegistry.readBuilder(task.file().format(), Record.class, input);

    return builder
        .project(table.schema())
        .idToConstant(partition)
        .split(task.start(), task.length())
        .caseSensitive(true)
        .build();
  }

  public static class CompactionResult {
    static final CompactionResult EMPTY = new CompactionResult(0, 0, 0);

    private final int filesRemoved;
    private final int filesAdded;
    private final int snapshotsExpired;

    CompactionResult(int filesRemoved, int filesAdded, int snapshotsExpired) {
      this.filesRemoved = filesRemoved;
      this.filesAdded = filesAdded;
      this.snapshotsExpired = snapshotsExpired;
    }

    public int filesRemoved() {
      return filesRemoved;
    }

    public int filesAdded() {
      return filesAdded;
    }

    public int snapshotsExpired() {
      return snapshotsExpired;
    }

    public boolean hasChanges() {
      return filesRemoved > 0;
    }

    @Override
    public String toString() {
      return String.format(
          java.util.Locale.ROOT,
          "CompactionResult{removed=%d, added=%d, snapshotsExpired=%d}",
          filesRemoved,
          filesAdded,
          snapshotsExpired);
    }
  }
}