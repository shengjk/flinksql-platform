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

import com.google.common.collect.Lists;
import io.github.shengjk.parse.Flink112Shims;
import io.github.shengjk.parse.SqlCommandParser;
import io.github.shengjk.parse.SqlSplitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author shengjk1
 * @date 2021/1/13
 */
public class SqlExecer {

	private final static Logger LOGGER = LoggerFactory.getLogger(Main.class);

	private Flink112Shims             flink112Shims;
	private StreamTableEnvironment    tableEnv;
	private String                    sqls;
	private Map<String, ConfigOption> configOptions;

	public SqlExecer(StreamTableEnvironment tableEnv, String sqls) {
		this.flink112Shims = new Flink112Shims();
		this.tableEnv = tableEnv;
		this.sqls = sqls;
		configOptions = flink112Shims.extractTableConfigOptions();
	}

	public void run(String comment) throws Exception {
		SqlCommandParser sqlCommandParser = new SqlCommandParser(flink112Shims, tableEnv);

		List<String> sqls = new SqlSplitter(comment).splitSql(this.sqls);
		for (String s : sqls) {
			Optional<SqlCommandParser.SqlCommandCall> sqlCommandCall = sqlCommandParser.parse(s);
			this.callCommand(sqlCommandCall.get());
		}
	}


	private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) throws Exception {
		switch (cmdCall.command) {
			case HELP:
				callHelp();
				break;
			case SELECT:
				callSelect(cmdCall.operands[0]);
				break;
			case SHOW_CATALOGS:
				callShowCatalogs();
				break;
			case SHOW_CURRENT_CATALOG:
				callShowCurrentCatalog();
				break;
			case SHOW_DATABASES:
				callShowDatabases();
				break;
			case SHOW_CURRENT_DATABASE:
				callShowCurrentDatabase();
				break;
			case SHOW_TABLES:
				callShowTables();
				break;
			case CREATE_FUNCTION:
				callCreateFunction(cmdCall.operands[0]);
				break;
			case DROP_FUNCTION:
				callDropFunction(cmdCall.operands[0]);
				break;
			case ALTER_FUNCTION:
				callAlterFunction(cmdCall.operands[0]);
				break;
			case SHOW_FUNCTIONS:
				callShowFunctions();
				break;
			case SHOW_MODULES:
				callShowModules();
				break;
			case USE_CATALOG:
				callUseCatalog(cmdCall.operands[0]);
				break;
			case USE:
				callUseDatabase(cmdCall.operands[0]);
				break;
			case CREATE_CATALOG:
				callCreateCatalog(cmdCall);
				break;
			case DROP_CATALOG:
				callDropCatalog(cmdCall.operands[0]);
				break;
			case DESC:
			case DESCRIBE:
				callDescribe(cmdCall.operands[0]);
				break;
			case EXPLAIN:
				callExplain(cmdCall.operands[0]);
				break;
			case SET:
				callSet(cmdCall.operands[0], cmdCall.operands[1]);
				break;
			case INSERT_INTO:
			case INSERT_OVERWRITE:
				callInsertInto(cmdCall.operands[0]);
				break;
			case CREATE_TABLE:
				callCreateTable(cmdCall.operands[0]);
				break;
			case DROP_TABLE:
				callDropTable(cmdCall.operands[0]);
				break;
			case CREATE_VIEW:
				callCreateView(cmdCall);
				break;
			case DROP_VIEW:
				callDropView(cmdCall.operands[0]);
				break;
			case CREATE_DATABASE:
				callCreateDatabase(cmdCall.operands[0]);
				break;
			case DROP_DATABASE:
				callDropDatabase(cmdCall.operands[0]);
				break;
			case ALTER_DATABASE:
				callAlterDatabase(cmdCall.operands[0]);
				break;
			case ALTER_TABLE:
				callAlterTable(cmdCall.operands[0]);
				break;
			default:
				throw new Exception("Unsupported command: " + cmdCall.command);
		}
	}

	private void callAlterTable(String sql) {
		this.tableEnv.executeSql(sql);
		LOGGER.info("Table has been modified.");
	}

	private void callAlterDatabase(String sql) {
		this.tableEnv.executeSql(sql);
		LOGGER.info("Table has been modified.");
	}

	private void callDropDatabase(String sql) {
		this.tableEnv.executeSql(sql);
		LOGGER.info("Table has been modified.");
	}

	private void callCreateDatabase(String sql) {
		this.tableEnv.executeSql(sql);
		LOGGER.info("Table has been modified.");
	}

	private void callDropView(String view) {
		this.tableEnv.dropTemporaryView(view);
		LOGGER.info("Table has been modified.");
	}

	private void callCreateView(SqlCommandParser.SqlCommandCall sqlCommand) {
		this.tableEnv.executeSql(sqlCommand.sql);
		LOGGER.info("View has been created.");
	}

	private void callCreateTable(String sql) {
		this.tableEnv.executeSql(sql);
		LOGGER.info("Table has been created.");
	}

	private void callDropTable(String sql) {
		this.tableEnv.executeSql(sql);
		LOGGER.info("Table has been dropped.");
	}

	private void callUseCatalog(String catalog) {
		this.tableEnv.useCatalog(catalog);
	}

	private void callCreateCatalog(SqlCommandParser.SqlCommandCall sqlCommand) {
		String sql = sqlCommand.sql;
		String operand = sqlCommand.operands[0];
		if (!operand.contains("hive-conf-dir")) {
			String hiveConfDir = System.getenv("HIVE_CONF_DIR");
			if (Objects.isNull(hiveConfDir)) {
				throw new FlinkHiveException("not find hive conf dir");
			}
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append(operand);
			stringBuilder.append(",");
			stringBuilder.append("\'");
			stringBuilder.append("hive-conf-dir");
			stringBuilder.append("\'");
			stringBuilder.append("=");
			stringBuilder.append("\'");
			stringBuilder.append(hiveConfDir);
			stringBuilder.append("\'");
			LOGGER.info("hive with {}", stringBuilder.toString());
			sql.replace(operand, stringBuilder.toString());
		}
		this.tableEnv.executeSql(sql);
		LOGGER.info("Catalog has been created.");
	}

	private void callDropCatalog(String sql) {
		this.tableEnv.executeSql(sql);
		LOGGER.info("Catalog has been dropped.");
	}

	private void callShowModules() {
		String[] modules = this.tableEnv.listModules();
		LOGGER.info("%table module" + StringUtils.join(modules, ","));
	}

	private void callHelp() {
		LOGGER.info(this.flink112Shims.sqlHelp());
	}

	private void callSelect(String sql) {
		TableResult tableResult = this.tableEnv.executeSql(sql);
		tableResult.print();
	}

	private void callShowCatalogs() {
		String[] catalogs = this.tableEnv.listCatalogs();
		LOGGER.info("all catalogs: {} ", StringUtils.join(catalogs, ","));
	}

	private void callShowCurrentCatalog() {
		String catalog = this.tableEnv.getCurrentCatalog();
		LOGGER.info("current catalog: {}", catalog);
	}

	private void callShowDatabases() {
		String[] databases = this.tableEnv.listDatabases();
		LOGGER.info("all databases: {}", StringUtils.join(databases, ","));
	}

	private void callShowCurrentDatabase() {
		String database = this.tableEnv.getCurrentDatabase();
		LOGGER.info("current database: {}", database);
	}

	private void callShowTables() {
		List<String> tables =
				Lists.newArrayList(this.tableEnv.listTables()).stream()
						.filter(tbl -> !tbl.startsWith("UnnamedTable")).collect(Collectors.toList());
		LOGGER.info("all tables: {} ", StringUtils.join(tables, ","));
	}

	private void callCreateFunction(String sql) {
		this.tableEnv.executeSql(sql);
	}

	private void callDropFunction(String sql) {
		this.tableEnv.executeSql(sql);
		LOGGER.info("Function has been dropped.");
	}

	private void callAlterFunction(String sql) {
		this.tableEnv.executeSql(sql);
		LOGGER.info("Function has been modified.");
	}

	private void callShowFunctions() {
		String[] functions = this.tableEnv.listUserDefinedFunctions();
		LOGGER.info("all functions:{}" + StringUtils.join(functions, ","));
	}

	private void callUseDatabase(String databaseName) {
		this.tableEnv.useDatabase(databaseName);
	}

	private void callDescribe(String name) {
		TableSchema schema = this.tableEnv.scan(name).getSchema();
		StringBuilder builder = new StringBuilder();
		builder.append("Column\tType");
		for (int i = 0; i < schema.getFieldCount(); ++i) {
			builder.append(schema.getFieldName(i).get() + "\t" + schema.getFieldDataType(i).get());
		}
		LOGGER.info("%table " + builder.toString());
	}

	private void callExplain(String sql) {
		String explainDetail = this.tableEnv.explainSql(sql);
		LOGGER.info("explainDetail:{}", explainDetail);
	}


	private void callSet(String key, String value) throws IOException {
		if (!configOptions.containsKey(key)) {
			throw new IOException(key + " is not a valid table/sql config, please check link: " +
					"https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/config.html");
		}
		this.tableEnv.getConfig().getConfiguration().setString(key, value);
	}

	private void callInsertInto(String sql) {
		this.tableEnv.executeSql(sql);
	}
}
