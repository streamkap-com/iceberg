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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestUnpartitionedDeltaWriter extends WriterTestBase {

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testInsert(String format) {
    IcebergSinkConfig config = cdcConfig(format);

    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 123L);
    row.setField("data", "hello");
    row.setField("id2", 123L);

    WriteResult result = writeTest(ImmutableList.of(row), config, UnpartitionedDeltaWriter.class);

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.dataFiles()).allMatch(f -> f.format() == FileFormat.fromString(format));
    assertThat(result.deleteFiles()).hasSize(0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testUpdateProducesDataAndDelete(String format) {
    IcebergSinkConfig config = cdcConfig(format);

    Record insert = GenericRecord.create(SCHEMA);
    insert.setField("id", 123L);
    insert.setField("data", "hello");
    insert.setField("id2", 123L);

    Record update = new RecordWrapper(createRow(123L, "updated", 123L), Operation.UPDATE);

    WriteResult result =
        writeTest(ImmutableList.of(insert, update), config, UnpartitionedDeltaWriter.class);

    assertThat(result.dataFiles()).hasSize(1);
    // The update should produce a positional delete (within same file) rather than eq delete
    // since the row was inserted in the same writer session
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testDeleteProducesEqualityDelete(String format) {
    IcebergSinkConfig config = cdcConfig(format);

    Record delete = new RecordWrapper(createRow(123L, "hello", 123L), Operation.DELETE);

    WriteResult result =
        writeTest(ImmutableList.of(delete), config, UnpartitionedDeltaWriter.class);

    // DELETE of a row not in current write session → equality delete file
    assertThat(result.dataFiles()).hasSize(0);
    assertThat(result.deleteFiles()).hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testUpsertMode(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.upsertModeEnabled()).thenReturn(true);

    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 123L);
    row.setField("data", "hello");
    row.setField("id2", 123L);

    WriteResult result = writeTest(ImmutableList.of(row), config, UnpartitionedDeltaWriter.class);

    // Upsert mode: each row is UPDATE → delete + write
    // Since this is a new row (not in current session), produces eq delete + data
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);
  }

  private Record createRow(long id, String data, long id2) {
    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", id);
    row.setField("data", data);
    row.setField("id2", id2);
    return row;
  }

  private IcebergSinkConfig cdcConfig(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));
    when(config.tablesCdcField()).thenReturn("__op");
    return config;
  }
}
