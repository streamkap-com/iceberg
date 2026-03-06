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
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final boolean upsertMode;
  private final boolean hardDeleteEnabled;
  private final String hardDeleteField;

  BaseDeltaTaskWriter(
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
    super(spec, format, writerFactory, fileFactory, io, targetFileSize);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds));
    this.upsertMode = upsertMode;
    this.hardDeleteEnabled = hardDeleteEnabled;
    this.hardDeleteField = hardDeleteField;
  }

  protected abstract RowDataDeltaWriter route(Record row);

  Schema schema() {
    return schema;
  }

  Schema deleteSchema() {
    return deleteSchema;
  }

  @Override
  public void write(Record record) throws IOException {
    Operation op;
    Record row;

    if (record instanceof RecordWrapper) {
      RecordWrapper wrapper = (RecordWrapper) record;
      op = wrapper.operation();
      row = wrapper.delegate();
    } else {
      row = record;
      if (hardDeleteEnabled && isHardDelete(row)) {
        op = Operation.DELETE;
      } else if (upsertMode || hardDeleteEnabled) {
        // hard delete enabled implies upsert for non-deleted records (current-state semantics)
        op = Operation.UPDATE;
      } else {
        op = Operation.INSERT;
      }
    }

    RowDataDeltaWriter writer = route(row);

    switch (op) {
      case INSERT:
        writer.write(row);
        break;
      case UPDATE:
        writer.delete(row);
        writer.write(row);
        break;
      case DELETE:
        writer.delete(row);
        break;
      default:
        throw new UnsupportedOperationException("Unknown operation: " + op);
    }
  }

  private boolean isHardDelete(Record row) {
    try {
      Object value = row.getField(hardDeleteField);
      if (value == null) {
        return false;
      }
      return Boolean.parseBoolean(value.toString());
    } catch (Exception e) {
      // Field not present in schema — treat as not deleted
      return false;
    }
  }

  protected class RowDataDeltaWriter extends BaseEqualityDeltaWriter {
    private final InternalRecordWrapper wrapper;

    RowDataDeltaWriter(StructLike partition) {
      super(partition, schema, deleteSchema);
      this.wrapper = new InternalRecordWrapper(schema.asStruct());
    }

    @Override
    protected StructLike asStructLike(Record data) {
      return wrapper.wrap(data);
    }

    @Override
    protected StructLike asStructLikeKey(Record key) {
      return wrapper.wrap(key);
    }
  }
}
