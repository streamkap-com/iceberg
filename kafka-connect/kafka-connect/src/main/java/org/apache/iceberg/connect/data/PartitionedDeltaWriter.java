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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

class PartitionedDeltaWriter extends BaseDeltaTaskWriter {

  private final Map<PartitionKey, RowDataDeltaWriter> writers;
  private final PartitionKey partitionKey;
  private final InternalRecordWrapper wrapper;

  PartitionedDeltaWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<Record> writerFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Set<Integer> identifierFieldIds,
      boolean upsertMode,
      boolean hardDeleteEnabled,
      String hardDeleteField) {
    super(
        spec,
        format,
        writerFactory,
        fileFactory,
        io,
        targetFileSize,
        schema,
        identifierFieldIds,
        upsertMode,
        hardDeleteEnabled,
        hardDeleteField);
    this.writers = Maps.newHashMap();
    this.partitionKey = new PartitionKey(spec, schema);
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
  }

  @Override
  protected RowDataDeltaWriter route(Record row) {
    partitionKey.partition(wrapper.wrap(row));

    RowDataDeltaWriter writer = writers.get(partitionKey);
    if (writer == null) {
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RowDataDeltaWriter(copiedKey);
      writers.put(copiedKey, writer);
    }
    return writer;
  }

  @Override
  public void close() throws IOException {
    for (RowDataDeltaWriter writer : writers.values()) {
      writer.close();
    }
    writers.clear();
    super.close();
  }
}
