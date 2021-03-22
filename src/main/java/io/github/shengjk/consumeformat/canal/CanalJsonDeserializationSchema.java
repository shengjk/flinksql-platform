/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.shengjk.consumeformat.canal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 *
 */
public final class CanalJsonDeserializationSchema implements DeserializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	private static final String OP_INSERT = "INSERT";
	private static final String OP_UPDATE = "UPDATE";
	private static final String OP_DELETE = "DELETE";

	private static final String SS_CANAL_AFTER  = "afterRow";
	private static final String SS_CANAL_BEFORE = "beforeRow";


	/**
	 * The deserializer to deserialize Debezium JSON data.
	 */
	private final JsonRowDataDeserializationSchema jsonDeserializer;

	/**
	 * TypeInformation of the produced {@link RowData}. *
	 */
	private final TypeInformation<RowData> resultTypeInfo;

	/**
	 * Only read changelogs from the specific database.
	 */
	private final @Nullable
	String database;

	/**
	 * Only read changelogs from the specific table.
	 */
	private final @Nullable
	String table;

	/**
	 * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
	 */
	private final boolean ignoreParseErrors;

	/**
	 * Number of fields.
	 */
	private final int fieldCount;

	private CanalJsonDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			@Nullable String database,
			@Nullable String table,
			boolean ignoreParseErrors,
			TimestampFormat timestampFormatOption) {
		this.resultTypeInfo = resultTypeInfo;
		this.database = database;
		this.table = table;
		this.ignoreParseErrors = ignoreParseErrors;
		this.fieldCount = rowType.getFieldCount();
		this.jsonDeserializer =
				new JsonRowDataDeserializationSchema(
						createJsonRowType(fromLogicalToDataType(rowType)),
						// the result type is never used, so it's fine to pass in Canal's result
						// type
						resultTypeInfo,
						false, // ignoreParseErrors already contains the functionality of
						// failOnMissingField
						ignoreParseErrors,
						timestampFormatOption);
	}

	// ------------------------------------------------------------------------------------------
	// Builder
	// ------------------------------------------------------------------------------------------

	/**
	 * Creates A builder for building a {@link CanalJsonDeserializationSchema}.
	 */
	public static Builder builder(RowType rowType, TypeInformation<RowData> resultTypeInfo) {
		return new Builder(rowType, resultTypeInfo);
	}

	/**
	 * A builder for creating a {@link CanalJsonDeserializationSchema}.
	 */
	@Internal
	public static final class Builder {
		private final RowType                  rowType;
		private final TypeInformation<RowData> resultTypeInfo;
		private       String                   database          = null;
		private       String                   table             = null;
		private       boolean                  ignoreParseErrors = false;
		private       TimestampFormat          timestampFormat   = TimestampFormat.SQL;

		private Builder(RowType rowType, TypeInformation<RowData> resultTypeInfo) {
			this.rowType = rowType;
			this.resultTypeInfo = resultTypeInfo;
		}

		public Builder setDatabase(String database) {
			this.database = database;
			return this;
		}

		public Builder setTable(String table) {
			this.table = table;
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public Builder setTimestampFormat(TimestampFormat timestampFormat) {
			this.timestampFormat = timestampFormat;
			return this;
		}

		public CanalJsonDeserializationSchema build() {
			return new CanalJsonDeserializationSchema(
					rowType, resultTypeInfo, database, table, ignoreParseErrors, timestampFormat);
		}
	}

	// ------------------------------------------------------------------------------------------

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		throw new RuntimeException(
				"Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
	}

	@Override
	public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
		try {
			RowData row = jsonDeserializer.deserialize(message);
			if (database != null) {
				String currentDatabase = row.getString(1).toString();
				if (!database.equals(currentDatabase)) {
					return;
				}
			}
			if (table != null) {
				String currentTable = row.getString(0).toString();
				if (!table.equals(currentTable)) {
					return;
				}
			}
			String type = row.getString(3).toString(); // "type" field
			if (OP_INSERT.equals(type)) {
				// "data" field is an array of row, contains inserted rows
				GenericMapData data = (GenericMapData) row.getMap(2);
				GenericRowData afterRow = (GenericRowData) data.get(BinaryStringData.fromString(SS_CANAL_AFTER));
				afterRow.setRowKind(RowKind.INSERT);
				out.collect(afterRow);
			} else if (OP_UPDATE.equals(type)) {
				GenericMapData data = (GenericMapData) row.getMap(2);
				GenericRowData beforeRow = (GenericRowData) data.get(BinaryStringData.fromString(SS_CANAL_BEFORE));
				beforeRow.setRowKind(RowKind.UPDATE_BEFORE);
				out.collect(beforeRow);
				GenericRowData afterRow = (GenericRowData) data.get(BinaryStringData.fromString(SS_CANAL_AFTER));
				afterRow.setRowKind(RowKind.UPDATE_AFTER);
				out.collect(afterRow);
			} else if (OP_DELETE.equals(type)) {
				// "data" field is an array of row, contains deleted rows
				GenericMapData data = (GenericMapData) row.getMap(2);
				GenericRowData beforeRow = (GenericRowData) data.get(BinaryStringData.fromString(SS_CANAL_BEFORE));
				beforeRow.setRowKind(RowKind.DELETE);
				out.collect(beforeRow);
			} else {
				// other type, we should skip it.
				return;
			}
		} catch (Throwable t) {
			// a big try catch to protect the processing.
			if (!ignoreParseErrors) {
				throw new IOException(
						format("Corrupt Canal JSON message '%s'.", new String(message)), t);
			}
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return resultTypeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CanalJsonDeserializationSchema that = (CanalJsonDeserializationSchema) o;
		return ignoreParseErrors == that.ignoreParseErrors
				&& fieldCount == that.fieldCount
				&& Objects.equals(jsonDeserializer, that.jsonDeserializer)
				&& Objects.equals(resultTypeInfo, that.resultTypeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(jsonDeserializer, resultTypeInfo, ignoreParseErrors, fieldCount);
	}

	private static RowType createJsonRowType(DataType databaseSchema) {
		// Canal JSON contains other information, e.g. "ts", "sql", but we don't need them
		return (RowType)
				DataTypes.ROW(
						DataTypes.FIELD("tableName", DataTypes.STRING()),
						DataTypes.FIELD("schemaName", DataTypes.STRING()),
						// 要查询的字段信息，都在 databaseSchema 中
						DataTypes.FIELD("rowData", DataTypes.MAP(DataTypes.STRING(), databaseSchema)),
						DataTypes.FIELD("eventType", DataTypes.STRING()))
						.getLogicalType();
	}
}
