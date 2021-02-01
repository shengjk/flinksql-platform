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

package io.github.shengjk.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static io.github.shengjk.redis.RedisTableSinkFactory.*;


public class RedisSink implements DynamicTableSink {


	private final DataType       type;
	private final ReadableConfig options;

	protected RedisSink(DataType type, ReadableConfig options) {
		this.type = type;
		this.options = options;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
//			return requestedMode;
		return ChangelogMode.newBuilder()
				.addContainedKind(RowKind.INSERT)
				.addContainedKind(RowKind.DELETE)
				.addContainedKind(RowKind.UPDATE_AFTER)
				.addContainedKind(RowKind.UPDATE_BEFORE)
				.build();
	}


	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		DataStructureConverter converter = context.createDataStructureConverter(type);
		return SinkFunctionProvider.of(new RedisSourceSinkFunction(converter, options, type));
	}

	@Override
	// not used
	public DynamicTableSink copy() {
		return new RedisSink(this.type, this.options);
	}

	@Override
	public String asSummaryString() {
		return "redisSink";
	}

	private static class RedisSourceSinkFunction extends RichSinkFunction<RowData> {

		private static final long serialVersionUID = 1L;

		private static final String HASH                = "hash";
		private static final String SET                 = "set";
		private static final String SADD                = "sadd";
		private static final String ZADD                = "zadd";
		private static final String RPUSH               = "rpush";
		private static final String LPUSH               = "lpush";
		private static final String PFADD               = "pfadd";
		private static final String PUBLISH             = "publish";
		private static final String EVALUATE_SYMBOL_PRE = "${";
		private static final String EVALUATE_SYMBOL_SUF = "}";

		private final DataStructureConverter           converter;
		private final ReadableConfig                   options;
		private final DataType                         type;
		private       RowType                          logicalType;
		private       HashMap<String, Integer>         fields;
		private       HashMap<String, LogicalTypeRoot> fieldTypes;
		private       JedisCluster                     jedisCluster;

		private RedisSourceSinkFunction(
				DataStructureConverter converter, ReadableConfig options, DataType type) {
			this.converter = converter;
			this.options = options;
			this.type = type;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			logicalType = (RowType) type.getLogicalType();
			fields = new HashMap<>();
			fieldTypes = new HashMap<>();
			List<RowType.RowField> rowFields = logicalType.getFields();
			int size = rowFields.size();
			for (int i = 0; i < size; i++) {
				String fileName = rowFields.get(i).getName();
				fields.put(fileName, i);
				fieldTypes.put(fileName, rowFields.get(i).getType().getTypeRoot());
			}
			if (options.getOptional(PASSWORD).orElse("").length() == 0) {
				jedisCluster = RedisUtil.getJedisCluster(options.get(HOST_PORT));
			} else {
				jedisCluster = RedisUtil.getJedisCluster(options.get(HOST_PORT), options.get(PASSWORD));
			}
		}

		@Override
		public void close() throws Exception {
			RedisUtil.closeConn(jedisCluster);
		}

		@Override
		public void invoke(RowData rowData, Context context) {
			RowKind rowKind = rowData.getRowKind();
			Row data = (Row) converter.toExternal(rowData);
			if (rowKind.equals(RowKind.UPDATE_AFTER) || rowKind.equals(RowKind.INSERT)) {

				String keyTemplate = options.get(KEY_TEMPLATE);
				if (Objects.isNull(keyTemplate) || keyTemplate.trim().length() == 0) {
					throw new NullPointerException(" keyTemplate is null or keyTemplate is empty");
				}

				if (keyTemplate.contains(EVALUATE_SYMBOL_PRE)) {
					String[] split = keyTemplate.split("\\$\\{");
					keyTemplate = "";
					for (String s : split) {
						if (s.contains(EVALUATE_SYMBOL_SUF)) {
							String filedName = s.substring(0, s.length() - 1);
							int index = fields.get(filedName);
							keyTemplate = keyTemplate + String.valueOf(data.getField(index));
						} else {
							keyTemplate = keyTemplate + s;
						}
					}
				}

				String keyType = options.get(KEY_TYPE);
				String valueNames = options.get(VALUE_NAMES);
				if (HASH.equalsIgnoreCase(keyType)) {
					String fieldTemplate = options.get(FIELD_TEMPLATE);
					if (fieldTemplate.contains(EVALUATE_SYMBOL_PRE)) {
						String[] split = fieldTemplate.split("\\$\\{");
						fieldTemplate = "";
						for (String s : split) {
							if (s.contains(EVALUATE_SYMBOL_SUF)) {
								String fieldName = s.substring(0, s.length() - 1);
								int index = fields.get(fieldName);
								fieldTemplate = fieldTemplate + String.valueOf(data.getField(index));
							} else {
								fieldTemplate = fieldTemplate + s;
							}
						}
					}

					if (valueNames.contains(",")) {
						HashMap<String, String> map = new HashMap<>();
						String[] fieldNames = valueNames.split(",");
						for (String fieldName : fieldNames) {
							String value = String.valueOf(data.getField(fields.get(fieldName)));
							map.put(fieldTemplate + "-" + fieldName, value);
						}
						jedisCluster.hset(keyTemplate, map);
					} else {
						jedisCluster.hset(keyTemplate, fieldTemplate + "-" + valueNames, String.valueOf(data.getField(fields.get(valueNames))));
					}

				} else if (SET.equalsIgnoreCase(keyType)) {
					jedisCluster.set(keyTemplate, String.valueOf(data.getField(fields.get(valueNames))));

				} else if (SADD.equalsIgnoreCase(keyType)) {
					jedisCluster.sadd(keyTemplate, String.valueOf(data.getField(fields.get(valueNames))));
				} else if (ZADD.equalsIgnoreCase(keyType)) {
					jedisCluster.sadd(keyTemplate, String.valueOf(data.getField(fields.get(valueNames))));
				} else if (RPUSH.equalsIgnoreCase(keyType)) {
					jedisCluster.rpush(keyTemplate, String.valueOf(data.getField(fields.get(valueNames))));
				} else if (LPUSH.equalsIgnoreCase(keyType)) {
					jedisCluster.lpush(keyTemplate, String.valueOf(data.getField(fields.get(valueNames))));
				} else if (PFADD.equalsIgnoreCase(keyType)) {
					jedisCluster.pfadd(keyTemplate, String.valueOf(data.getField(fields.get(valueNames))));
				} else if (PUBLISH.equalsIgnoreCase(keyType)) {
					jedisCluster.publish(keyTemplate, String.valueOf(data.getField(fields.get(valueNames))));
				} else {
					throw new IllegalArgumentException(" not find this keyType:" + keyType);
				}

				if (Objects.nonNull(options.get(EXPIRE_TIME))) {
					jedisCluster.expire(keyTemplate, options.get(EXPIRE_TIME));
				}
			} else {
				String keyTemplate = options.get(KEY_TEMPLATE);
				if (Objects.isNull(keyTemplate) || keyTemplate.trim().length() == 0) {
					throw new NullPointerException(" keyTemplate is null or keyTemplate is empty");
				}

				if (keyTemplate.contains(EVALUATE_SYMBOL_PRE)) {
					String[] split = keyTemplate.split("\\$\\{");
					keyTemplate = "";
					for (String s : split) {
						if (s.contains(EVALUATE_SYMBOL_SUF)) {
							String filedName = s.substring(0, s.length() - 1);
							int index = fields.get(filedName);
							keyTemplate = keyTemplate + String.valueOf(data.getField(index));
						} else {
							keyTemplate = keyTemplate + s;
						}
					}
				}

				String keyType = options.get(KEY_TYPE);
				String valueNames = options.get(VALUE_NAMES);
				if (HASH.equalsIgnoreCase(keyType)) {
					String fieldTemplate = options.get(FIELD_TEMPLATE);
					if (fieldTemplate.contains(EVALUATE_SYMBOL_PRE)) {
						String[] split = fieldTemplate.split("\\$\\{");
						fieldTemplate = "";
						for (String s : split) {
							if (s.contains(EVALUATE_SYMBOL_SUF)) {
								String fieldName = s.substring(0, s.length() - 1);
								int index = fields.get(fieldName);
								fieldTemplate = fieldTemplate + String.valueOf(data.getField(index));
							} else {
								fieldTemplate = fieldTemplate + s;
							}
						}
					}

					String[] fieldNames = valueNames.split(",");
					for (String fieldName : fieldNames) {
						LogicalTypeRoot logicalTypeRoot = fieldTypes.get(fieldName);
						if (logicalTypeRoot == LogicalTypeRoot.BIGINT
								|| logicalTypeRoot == LogicalTypeRoot.INTEGER) {
							jedisCluster.hincrBy(keyTemplate, fieldTemplate + "-" + fieldName, -Long.parseLong(data.getField(fields.get(fieldName)).toString()));
						} else if (logicalTypeRoot == LogicalTypeRoot.DOUBLE
								|| logicalTypeRoot == LogicalTypeRoot.FLOAT) {
							jedisCluster.hincrByFloat(keyTemplate, fieldTemplate + "-" + fieldName, -Double.parseDouble(data.getField(fields.get(fieldName)).toString()));
						}
					}
				}
			}
		}
	}
}