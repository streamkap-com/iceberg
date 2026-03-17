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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Tests that verify Parquet min/max column statistics behavior with different metrics modes.
 *
 * <p>MetricsConfig is built from table.properties() via MetricsConfig.forTable(table), so metrics
 * mode must be set as TABLE properties (e.g., via auto-create-props or ALTER TABLE), not via
 * write-props. Write-props flow to the Parquet writer but do NOT affect MetricsConfig.
 */
public class TestWriterMetricsMode extends WriterTestBase {

  private static final int DATA_FIELD_ID = 2;

  private static String generateString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append((char) ('a' + (i % 26)));
    }
    return sb.toString();
  }

  private IcebergSinkConfig defaultConfig() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of());
    return config;
  }

  /**
   * Sets table properties on the mock table. MetricsConfig reads from table.properties(), so this
   * is how metrics mode is controlled.
   */
  private void setTableProperties(Map<String, String> props) {
    when(table.properties()).thenReturn(props);
  }

  @Test
  public void testDefaultTruncateProducesBounds() {
    // Default mode is truncate(16) — should produce lower/upper bounds
    String longValue = generateString(100);
    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", longValue);
    row.setField("id2", 1L);

    WriteResult result = writeTest(ImmutableList.of(row), defaultConfig(), UnpartitionedWriter.class);
    DataFile file = result.dataFiles()[0];

    assertThat(file.lowerBounds()).containsKey(DATA_FIELD_ID);
    assertThat(file.upperBounds()).containsKey(DATA_FIELD_ID);

    // bounds should be truncated to at most 16 characters
    String lowerBound =
        Conversions.fromByteBuffer(Types.StringType.get(), file.lowerBounds().get(DATA_FIELD_ID))
            .toString();
    assertThat(lowerBound.length()).isLessThanOrEqualTo(16);
  }

  @Test
  public void testCustomTruncateLength() {
    // truncate(8) via table properties — bounds must be at most 8 characters
    setTableProperties(
        ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(8)"));

    String longValue = generateString(200);
    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", longValue);
    row.setField("id2", 1L);

    WriteResult result = writeTest(ImmutableList.of(row), defaultConfig(), UnpartitionedWriter.class);
    DataFile file = result.dataFiles()[0];

    assertThat(file.lowerBounds()).containsKey(DATA_FIELD_ID);
    assertThat(file.upperBounds()).containsKey(DATA_FIELD_ID);

    String lowerBound =
        Conversions.fromByteBuffer(Types.StringType.get(), file.lowerBounds().get(DATA_FIELD_ID))
            .toString();
    String upperBound =
        Conversions.fromByteBuffer(Types.StringType.get(), file.upperBounds().get(DATA_FIELD_ID))
            .toString();

    assertThat(lowerBound.length()).isLessThanOrEqualTo(8);
    assertThat(upperBound.length()).isLessThanOrEqualTo(8);
  }

  @Test
  public void testCountsModeOmitsBounds() {
    // "counts" mode: value counts present, but NO lower/upper bounds
    setTableProperties(ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts"));

    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", generateString(100));
    row.setField("id2", 1L);

    WriteResult result = writeTest(ImmutableList.of(row), defaultConfig(), UnpartitionedWriter.class);
    DataFile file = result.dataFiles()[0];

    assertThat(file.valueCounts()).containsKey(DATA_FIELD_ID);
    assertThat(file.nullValueCounts()).containsKey(DATA_FIELD_ID);

    Map<Integer, ByteBuffer> lower = file.lowerBounds();
    assertThat(lower == null || !lower.containsKey(DATA_FIELD_ID))
        .as("counts mode should not produce lower bounds")
        .isTrue();

    Map<Integer, ByteBuffer> upper = file.upperBounds();
    assertThat(upper == null || !upper.containsKey(DATA_FIELD_ID))
        .as("counts mode should not produce upper bounds")
        .isTrue();
  }

  @Test
  public void testNoneModeOmitsAllMetrics() {
    // "none" mode: no metrics at all for any column
    setTableProperties(ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none"));

    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", generateString(50));
    row.setField("id2", 1L);

    WriteResult result = writeTest(ImmutableList.of(row), defaultConfig(), UnpartitionedWriter.class);
    DataFile file = result.dataFiles()[0];

    Map<Integer, Long> valueCounts = file.valueCounts();
    assertThat(valueCounts == null || !valueCounts.containsKey(DATA_FIELD_ID))
        .as("none mode should not produce value counts")
        .isTrue();

    Map<Integer, ByteBuffer> lower = file.lowerBounds();
    assertThat(lower == null || !lower.containsKey(DATA_FIELD_ID))
        .as("none mode should not produce lower bounds")
        .isTrue();
  }

  @Test
  public void testFullModePreservesCompleteBounds() {
    // "full" mode: complete (untruncated) bounds
    setTableProperties(ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "full"));

    String value = generateString(100);
    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", value);
    row.setField("id2", 1L);

    WriteResult result = writeTest(ImmutableList.of(row), defaultConfig(), UnpartitionedWriter.class);
    DataFile file = result.dataFiles()[0];

    assertThat(file.lowerBounds()).containsKey(DATA_FIELD_ID);

    String lowerBound =
        Conversions.fromByteBuffer(Types.StringType.get(), file.lowerBounds().get(DATA_FIELD_ID))
            .toString();
    assertThat(lowerBound).isEqualTo(value);
  }

  @Test
  public void testPerColumnMetricsOverride() {
    // Global "full", but "data" column overridden to "none"
    setTableProperties(
        ImmutableMap.of(
            TableProperties.DEFAULT_WRITE_METRICS_MODE, "full",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data", "none"));

    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", generateString(100));
    row.setField("id2", 1L);

    WriteResult result = writeTest(ImmutableList.of(row), defaultConfig(), UnpartitionedWriter.class);
    DataFile file = result.dataFiles()[0];

    // "id" column (field 1) should have bounds (global = full)
    assertThat(file.lowerBounds()).containsKey(1);

    // "data" column (field 2) should NOT have bounds (overridden to none)
    Map<Integer, ByteBuffer> lower = file.lowerBounds();
    assertThat(lower == null || !lower.containsKey(DATA_FIELD_ID))
        .as("per-column none override should suppress bounds for data column")
        .isTrue();
  }

  @Test
  public void testLargeJsonFieldWithDefaultTruncation() {
    // Simulates the Cedar/Snowflake issue: a large JSON string gets truncated stats
    String largeJson =
        "{\"user\":{\"name\":\"John Doe\",\"email\":\"john@example.com\","
            + "\"preferences\":{\"theme\":\"dark\",\"language\":\"en\","
            + "\"notifications\":{\"email\":true,\"push\":false}},"
            + "\"tags\":[\"admin\",\"beta-tester\",\"premium\"]}}";

    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", largeJson);
    row.setField("id2", 1L);

    WriteResult result = writeTest(ImmutableList.of(row), defaultConfig(), UnpartitionedWriter.class);
    DataFile file = result.dataFiles()[0];

    // default truncate(16): bounds exist but are truncated — this is what causes Snowflake issues
    String lowerBound =
        Conversions.fromByteBuffer(Types.StringType.get(), file.lowerBounds().get(DATA_FIELD_ID))
            .toString();
    assertThat(lowerBound.length()).isLessThanOrEqualTo(16);
    assertThat(lowerBound.length()).isLessThan(largeJson.length());
  }

  @Test
  public void testLargeJsonFieldWithCountsMode() {
    // The fix for Cedar: use "counts" mode via table properties to avoid truncation issues
    setTableProperties(ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts"));

    String largeJson =
        "{\"user\":{\"name\":\"John Doe\",\"email\":\"john@example.com\","
            + "\"preferences\":{\"theme\":\"dark\",\"language\":\"en\","
            + "\"notifications\":{\"email\":true,\"push\":false}},"
            + "\"tags\":[\"admin\",\"beta-tester\",\"premium\"]}}";

    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 1L);
    row.setField("data", largeJson);
    row.setField("id2", 1L);

    WriteResult result = writeTest(ImmutableList.of(row), defaultConfig(), UnpartitionedWriter.class);
    DataFile file = result.dataFiles()[0];

    // counts mode: value counts present, but no bounds — Snowflake reads fine
    assertThat(file.valueCounts()).containsKey(DATA_FIELD_ID);

    Map<Integer, ByteBuffer> lower = file.lowerBounds();
    assertThat(lower == null || !lower.containsKey(DATA_FIELD_ID))
        .as("counts mode should not produce bounds for JSON field")
        .isTrue();
  }
}
