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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;


/**
 * @author shengjk1
 * @date 2020/10/16
 */
public class RedisTableSinkFactory implements DynamicTableSinkFactory {
	private final static Logger LOGGER = LoggerFactory.getLogger(RedisTableSinkFactory.class);

	public static final String IDENTIFIER = "redis";

	public static final ConfigOption<String> HOST_PORT = key("hostPort")
			.stringType()
			.noDefaultValue()
			.withDescription("redis host and port,");

	public static final ConfigOption<String> PASSWORD = key("password")
			.stringType()
			.noDefaultValue()
			.withDescription("redis password");

	public static final ConfigOption<Integer> EXPIRE_TIME = key("expireTime")
			.intType()
			.noDefaultValue()
			.withDescription("redis key expire time");

	public static final ConfigOption<String> KEY_TYPE = key("keyType")
			.stringType()
			.noDefaultValue()
			.withDescription("redis key type,such as hash,string and so on ");

	public static final ConfigOption<String> KEY_TEMPLATE = key("keyTemplate")
			.stringType()
			.noDefaultValue()
			.withDescription("redis key template ");

	public static final ConfigOption<String> FIELD_TEMPLATE = key("fieldTemplate")
			.stringType()
			.noDefaultValue()
			.withDescription("redis field template ");


	public static final ConfigOption<String> VALUE_NAMES = key("valueNames")
			.stringType()
			.noDefaultValue()
			.withDescription("redis value name ");

	public static final ConfigOption<Integer> MAX_RETRY_TIMES = key("maxRetryTimes")
			.intType()
			.defaultValue(1)
			.withDescription(" query redis retry times");

	public static final ConfigOption<Boolean> KEY_VALUE_IS_PART = key("keyNameValueIsPart")
			.booleanType()
			.defaultValue(false)
			.withDescription("");


	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(HOST_PORT);
		options.add(EXPIRE_TIME);
		options.add(KEY_TYPE);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PASSWORD);
		options.add(KEY_TEMPLATE);
		options.add(FIELD_TEMPLATE);
		options.add(VALUE_NAMES);
		return options;
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		//参数校验
		helper.validate();

		ReadableConfig options = helper.getOptions();
		return new RedisSink(
				context.getCatalogTable().getSchema().toPhysicalRowDataType(),
				options);
	}
}
