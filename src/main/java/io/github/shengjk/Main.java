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

package io.github.shengjk;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Objects;

/**
 * @author shengjk1
 * @date 2021/1/13
 */
public class Main {
	private final static Logger LOGGER = LoggerFactory.getLogger(Main.class);

	static int RESTARTAT_TEMPTS       = 4;
	static int DELAY_BETWEENAT_TEMPTS = 10 * 1000;

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(RESTARTAT_TEMPTS, DELAY_BETWEENAT_TEMPTS));

		//fool configuration
		EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

		// enable state ttl
		TableConfig config = tableEnv.getConfig();
		config.setIdleStateRetention(Duration.ofDays(1));
		// enable checkpointing
		Configuration configuration = config.getConfiguration();
		configuration.set(
				ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
		configuration.set(
				ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
		configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMinutes(30));

		configuration.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT, CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		// 可以通过 set 命令来配置
		// set low-level key-value options
		configuration.setString("table.exec.mini-batch.enabled", "true"); // enable mini-batch optimization
		configuration.setString("table.exec.mini-batch.allow-latency", "1 s"); // use 5 seconds to buffer input records
		configuration.setString("table.exec.mini-batch.size", "1000");
		configuration.setString("table.exec.sink.not-null-enforcer", "drop");

		String sqlPath = parameter.get("sqlPath");
		if (Objects.isNull(sqlPath)) {
			throw new IllegalArgumentException("args must include sqlPath");
		}
		LOGGER.info("sqlPath:{}", sqlPath);

		File sqlFile = new File(sqlPath);
		String sqls = FileUtils.readFileToString(sqlFile, Charset.forName("UTF-8"));
		SqlExecer sqlExecer = new SqlExecer(tableEnv, sqls);
		String comment = parameter.get("comment");
		LOGGER.info("comment:{} ", Objects.nonNull(comment) ? comment : "--");
		sqlExecer.run(Objects.nonNull(comment) ? comment : "--");
//		tableEnv.execute(parameter.get("jobName", ""));
		LOGGER.info("running......");
	}


}
