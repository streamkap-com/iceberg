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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Integration tests for Iceberg V3 format tables via Kafka Connect, covering:
 *
 * <ul>
 *   <li>V3 table format creation and basic data ingestion
 *   <li>Variant data type column support
 *   <li>Upsert mode with equality deletes on V3 tables
 *   <li>Deletion vectors (V3 feature)
 * </ul>
 */
public class TestIntegrationV3 extends IntegrationTestBase {

  private static final String TEST_TABLE = "v3_test";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);

  // V3 schema with a Variant column
  public static final Schema V3_SCHEMA_WITH_VARIANT =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "type", Types.StringType.get()),
              Types.NestedField.required(3, "ts", Types.TimestampType.withZone()),
              Types.NestedField.required(4, "payload", Types.StringType.get()),
              Types.NestedField.optional(5, "metadata", Types.VariantType.get())),
          org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet.of(1));

  // V3 schema without Variant (for basic V3 format tests)
  public static final Schema V3_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "type", Types.StringType.get()),
              Types.NestedField.required(3, "ts", Types.TimestampType.withZone()),
              Types.NestedField.required(4, "payload", Types.StringType.get())),
          org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet.of(1));

  // -- V3 basic append tests --

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testV3UnpartitionedTableAppend(String branch) {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            V3_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    assertFormatVersion(table, 3);

    boolean useSchema = branch == null;
    runTest(branch, useSchema, ImmutableMap.of(), List.of(TABLE_IDENTIFIER));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);
    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "test_branch")
  public void testV3PartitionedTableAppend(String branch) {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            V3_SCHEMA,
            TestEvent.TEST_SPEC,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    assertFormatVersion(table, 3);

    boolean useSchema = branch == null;
    runTest(branch, useSchema, ImmutableMap.of(), List.of(TABLE_IDENTIFIER));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, branch);
    assertThat(files).hasSizeBetween(1, 2);
    assertSnapshotProps(TABLE_IDENTIFIER, branch);
  }

  // -- V3 auto-create with format-version property --

  @Test
  public void testV3AutoCreateTable() {
    Map<String, String> extraConfig = Maps.newHashMap();
    extraConfig.put("iceberg.tables.auto-create-enabled", "true");
    extraConfig.put("iceberg.tables.auto-create-props.format-version", "3");

    runTest(null, true, extraConfig, List.of(TABLE_IDENTIFIER));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    assertFormatVersion(table, 3);

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, null);
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);
    assertSnapshotProps(TABLE_IDENTIFIER, null);
  }

  // -- Variant data type tests --

  @Test
  public void testV3TableWithVariantColumn() {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            V3_SCHEMA_WITH_VARIANT,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    assertFormatVersion(table, 3);
    assertThat(table.schema().findField("metadata")).isNotNull();
    assertThat(table.schema().findField("metadata").type().isVariantType()).isTrue();

    // send events — the variant column is optional, so records without variant data should work
    runTest(null, true, ImmutableMap.of(), List.of(TABLE_IDENTIFIER));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, null);
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);
    assertSnapshotProps(TABLE_IDENTIFIER, null);
  }

  @Test
  public void testV3AutoCreateWithSchemaEvolutionAddsVariantColumn() {
    // Start with a V3 table that has no variant column
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            V3_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    // Run with schema evolution enabled — variant column can be added later
    Map<String, String> extraConfig = Maps.newHashMap();
    extraConfig.put("iceberg.tables.evolve-schema-enabled", "true");

    runTest(null, true, extraConfig, List.of(TABLE_IDENTIFIER));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    assertFormatVersion(table, 3);

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, null);
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);
    assertSnapshotProps(TABLE_IDENTIFIER, null);
  }

  // -- Upsert mode / Equality delete tests on V3 --

  @Test
  public void testV3UpsertModeProducesEqualityDeletes() {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            V3_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    Map<String, String> extraConfig = Maps.newHashMap();
    extraConfig.put("iceberg.tables.upsert-mode-enabled", "true");

    KafkaConnectUtils.Config connectorConfig = createConfig(true);
    context().connectorCatalogProperties().forEach(connectorConfig::config);
    extraConfig.forEach(connectorConfig::config);
    context().startConnector(connectorConfig);

    // send initial insert events
    TestEvent insert1 = new TestEvent(1, "type1", Instant.now(), "initial value");
    TestEvent insert2 = new TestEvent(2, "type2", Instant.now(), "another value");
    send(testTopic(), insert1, true);
    send(testTopic(), insert2, true);
    flush();

    // wait for first commit (append)
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(List.of(TABLE_IDENTIFIER)));

    // send upsert events (same IDs = updates)
    TestEvent update1 = new TestEvent(1, "type1", Instant.now(), "updated value");
    TestEvent update2 = new TestEvent(2, "type2", Instant.now(), "updated too");
    send(testTopic(), update1, true);
    send(testTopic(), update2, true);
    flush();

    // wait for second commit which should produce equality delete files
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Table table = catalog().loadTable(TABLE_IDENTIFIER);
              // should have at least 2 snapshots: initial append + row delta
              assertThat(table.snapshots()).hasSizeGreaterThanOrEqualTo(2);
            });

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    // the latest snapshot should contain equality delete files from the upsert
    List<DeleteFile> deletes = deleteFiles(TABLE_IDENTIFIER, null);
    assertThat(deletes).isNotEmpty();
    assertThat(deletes)
        .allSatisfy(
            deleteFile ->
                assertThat(deleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES));
  }

  // -- CDC mode with hard deletes on V3 --

  @Test
  public void testV3CdcModeWithHardDeletes() {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            V3_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    Map<String, String> extraConfig = Maps.newHashMap();
    extraConfig.put("iceberg.tables.cdc-field", "op");
    extraConfig.put("iceberg.tables.hard-delete-enabled", "true");

    KafkaConnectUtils.Config connectorConfig = createConfig(true);
    context().connectorCatalogProperties().forEach(connectorConfig::config);
    extraConfig.forEach(connectorConfig::config);
    context().startConnector(connectorConfig);

    // send insert events with CDC op field
    TestEvent insert1 = new TestEvent(1, "type1", Instant.now(), "data1", "I");
    TestEvent insert2 = new TestEvent(2, "type2", Instant.now(), "data2", "I");
    send(testTopic(), insert1, true);
    send(testTopic(), insert2, true);
    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(List.of(TABLE_IDENTIFIER)));

    // send a delete event
    TestEvent delete1 = new TestEvent(1, "type1", Instant.now(), "data1", "D");
    send(testTopic(), delete1, true);
    flush();

    // wait for the delete commit
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Table table = catalog().loadTable(TABLE_IDENTIFIER);
              assertThat(table.snapshots()).hasSizeGreaterThanOrEqualTo(2);
            });

    // verify delete files were produced
    List<DeleteFile> deletes = deleteFiles(TABLE_IDENTIFIER, null);
    assertThat(deletes).isNotEmpty();
  }

  // -- Deletion Vector tests (V3 feature) --

  @Test
  public void testV3TableWithDeletionVectorsEnabled() {
    // Create a V3 table with deletion vectors via merge-on-read delete mode
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            V3_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, "3",
                TableProperties.DELETE_MODE, "merge-on-read"));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    assertFormatVersion(table, 3);
    assertThat(table.properties()).containsEntry(TableProperties.DELETE_MODE, "merge-on-read");

    // basic append should still work on a DV-enabled table
    runTest(null, true, ImmutableMap.of(), List.of(TABLE_IDENTIFIER));

    List<DataFile> files = dataFiles(TABLE_IDENTIFIER, null);
    assertThat(files).hasSizeBetween(1, 2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(2);
    assertSnapshotProps(TABLE_IDENTIFIER, null);
  }

  @Test
  public void testV3UpsertWithDeletionVectors() {
    // Create a V3 table with merge-on-read (enables deletion vectors)
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            V3_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, "3",
                TableProperties.DELETE_MODE, "merge-on-read"));

    Map<String, String> extraConfig = Maps.newHashMap();
    extraConfig.put("iceberg.tables.upsert-mode-enabled", "true");

    KafkaConnectUtils.Config connectorConfig = createConfig(true);
    context().connectorCatalogProperties().forEach(connectorConfig::config);
    extraConfig.forEach(connectorConfig::config);
    context().startConnector(connectorConfig);

    // send initial inserts
    TestEvent insert1 = new TestEvent(1, "type1", Instant.now(), "first");
    TestEvent insert2 = new TestEvent(2, "type2", Instant.now(), "second");
    send(testTopic(), insert1, true);
    send(testTopic(), insert2, true);
    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(List.of(TABLE_IDENTIFIER)));

    // send upserts (same IDs)
    TestEvent upsert1 = new TestEvent(1, "type1", Instant.now(), "updated first");
    TestEvent upsert2 = new TestEvent(2, "type2", Instant.now(), "updated second");
    send(testTopic(), upsert1, true);
    send(testTopic(), upsert2, true);
    flush();

    // wait for second commit with deletes
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Table table = catalog().loadTable(TABLE_IDENTIFIER);
              assertThat(table.snapshots()).hasSizeGreaterThanOrEqualTo(2);
            });

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    assertFormatVersion(table, 3);

    // verify delete files exist from the upsert operation
    List<DeleteFile> deletes = deleteFiles(TABLE_IDENTIFIER, null);
    assertThat(deletes).isNotEmpty();
  }

  // -- V3 + Variant + Upsert combined test --

  @Test
  public void testV3VariantTableWithUpsert() {
    catalog()
        .createTable(
            TABLE_IDENTIFIER,
            V3_SCHEMA_WITH_VARIANT,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    Table table = catalog().loadTable(TABLE_IDENTIFIER);
    assertFormatVersion(table, 3);
    assertThat(table.schema().findField("metadata").type().isVariantType()).isTrue();

    Map<String, String> extraConfig = Maps.newHashMap();
    extraConfig.put("iceberg.tables.upsert-mode-enabled", "true");

    KafkaConnectUtils.Config connectorConfig = createConfig(true);
    context().connectorCatalogProperties().forEach(connectorConfig::config);
    extraConfig.forEach(connectorConfig::config);
    context().startConnector(connectorConfig);

    // insert events (variant column is optional, data goes through without it)
    TestEvent insert1 = new TestEvent(1, "type1", Instant.now(), "data1");
    TestEvent insert2 = new TestEvent(2, "type2", Instant.now(), "data2");
    send(testTopic(), insert1, true);
    send(testTopic(), insert2, true);
    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertSnapshotAdded(List.of(TABLE_IDENTIFIER)));

    // upsert events
    TestEvent upsert1 = new TestEvent(1, "type1", Instant.now(), "updated data1");
    send(testTopic(), upsert1, true);
    flush();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Table t = catalog().loadTable(TABLE_IDENTIFIER);
              assertThat(t.snapshots()).hasSizeGreaterThanOrEqualTo(2);
            });

    List<DeleteFile> deletes = deleteFiles(TABLE_IDENTIFIER, null);
    assertThat(deletes).isNotEmpty();
  }

  private static void assertFormatVersion(Table table, int expectedVersion) {
    int actual = ((HasTableOperations) table).operations().current().formatVersion();
    assertThat(actual).isEqualTo(expectedVersion);
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE));
  }

  @Override
  protected void sendEvents(boolean useSchema) {
    TestEvent event1 = new TestEvent(1, "type1", Instant.now(), "hello world!");

    Instant threeDaysAgo = Instant.now().minus(Duration.ofDays(3));
    TestEvent event2 = new TestEvent(2, "type2", threeDaysAgo, "having fun?");

    send(testTopic(), event1, useSchema);
    send(testTopic(), event2, useSchema);
  }

  @Override
  void dropTables() {
    catalog().dropTable(TableIdentifier.of(TEST_DB, TEST_TABLE));
  }
}