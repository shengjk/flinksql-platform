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

package io.github.shengjk.parse;

import io.github.shengjk.parse.SqlCommandParser.SqlCommand;
import io.github.shengjk.parse.SqlCommandParser.SqlCommandCall;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.*;
import org.apache.flink.table.operations.ddl.*;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;


/**
 * Shims for flink 1.12
 * reference https://github.com/apache/zeppelin/blob/6f0b8bd262b87649dd38de5437d92662ae9b706f/flink/flink1.12-shims/src/main/java/org/apache/zeppelin/flink/Flink112Shims.java
 */
public class Flink112Shims {

	private static final Logger           LOGGER       = LoggerFactory.getLogger(Flink112Shims.class);
	public static final  AttributedString MESSAGE_HELP = new AttributedStringBuilder()
			.append("The following commands are available:\n\n")
			.append(formatCommand(SqlCommand.CREATE_TABLE, "Create table under current catalog and database."))
			.append(formatCommand(SqlCommand.DROP_TABLE, "Drop table with optional catalog and database. Syntax: 'DROP TABLE [IF EXISTS] <name>;'"))
			.append(formatCommand(SqlCommand.CREATE_VIEW, "Creates a virtual table from a SQL query. Syntax: 'CREATE VIEW <name> AS <query>;'"))
			.append(formatCommand(SqlCommand.DESCRIBE, "Describes the schema of a table with the given name."))
			.append(formatCommand(SqlCommand.DROP_VIEW, "Deletes a previously created virtual table. Syntax: 'DROP VIEW <name>;'"))
			.append(formatCommand(SqlCommand.EXPLAIN, "Describes the execution plan of a query or table with the given name."))
			.append(formatCommand(SqlCommand.HELP, "Prints the available commands."))
			.append(formatCommand(SqlCommand.INSERT_INTO, "Inserts the results of a SQL SELECT query into a declared table sink."))
			.append(formatCommand(SqlCommand.INSERT_OVERWRITE, "Inserts the results of a SQL SELECT query into a declared table sink and overwrite existing data."))
			.append(formatCommand(SqlCommand.SELECT, "Executes a SQL SELECT query on the Flink cluster."))
			.append(formatCommand(SqlCommand.SET, "Sets a session configuration property. Syntax: 'SET <key>=<value>;'. Use 'SET;' for listing all properties."))
			.append(formatCommand(SqlCommand.SHOW_FUNCTIONS, "Shows all user-defined and built-in functions."))
			.append(formatCommand(SqlCommand.SHOW_TABLES, "Shows all registered tables."))
			.append(formatCommand(SqlCommand.SOURCE, "Reads a SQL SELECT query from a file and executes it on the Flink cluster."))
			.append(formatCommand(SqlCommand.USE_CATALOG, "Sets the current catalog. The current database is set to the catalog's default one. Experimental! Syntax: 'USE CATALOG <name>;'"))
			.append(formatCommand(SqlCommand.USE, "Sets the current default database. Experimental! Syntax: 'USE <name>;'"))
			.style(AttributedStyle.DEFAULT.underline())
			.append("\nHint")
			.style(AttributedStyle.DEFAULT)
			.append(": Make sure that a statement ends with ';' for finalizing (multi-line) statements.")
			.toAttributedString();

	private Map<String, StatementSet> statementSetMap = new ConcurrentHashMap<>();

	public Flink112Shims() {

	}


	/**
	 * Parse it via flink SqlParser first, then fallback to regular expression matching.
	 *
	 * @param tableEnv
	 * @param stmt
	 * @return
	 */

	public Optional<SqlCommandParser.SqlCommandCall> parseSql(Object tableEnv, String stmt) {
		Parser sqlParser = ((TableEnvironmentInternal) tableEnv).getParser();
		SqlCommandCall sqlCommandCall = null;
		try {
			// parse statement via regex matching first
			Optional<SqlCommandCall> callOpt = parseByRegexMatching(stmt);
			if (callOpt.isPresent()) {
				sqlCommandCall = callOpt.get();
			} else {
				sqlCommandCall = parseBySqlParser(sqlParser, stmt);
			}
		} catch (Exception e) {
			LOGGER.error("sql:{} e:{}", stmt, e);
			return Optional.empty();
		}
		return Optional.of(sqlCommandCall);

	}

	private SqlCommandCall parseBySqlParser(Parser sqlParser, String stmt) throws Exception {
		List<Operation> operations;
		try {
			operations = sqlParser.parse(stmt);
		} catch (Throwable e) {
			throw new Exception("Invalidate SQL statement.", e);
		}
		if (operations.size() != 1) {
			throw new Exception("Only single statement is supported now.");
		}

		final SqlCommand cmd;
		String[] operands = new String[]{stmt};
		Operation operation = operations.get(0);
		if (operation instanceof CatalogSinkModifyOperation) {
			boolean overwrite = ((CatalogSinkModifyOperation) operation).isOverwrite();
			cmd = overwrite ? SqlCommand.INSERT_OVERWRITE : SqlCommand.INSERT_INTO;
		} else if (operation instanceof CreateTableOperation) {
			cmd = SqlCommand.CREATE_TABLE;
		} else if (operation instanceof DropTableOperation) {
			cmd = SqlCommand.DROP_TABLE;
		} else if (operation instanceof AlterTableOperation) {
			cmd = SqlCommand.ALTER_TABLE;
		} else if (operation instanceof CreateViewOperation) {
			cmd = SqlCommand.CREATE_VIEW;
		} else if (operation instanceof DropViewOperation) {
			cmd = SqlCommand.DROP_VIEW;
		} else if (operation instanceof CreateDatabaseOperation) {
			cmd = SqlCommand.CREATE_DATABASE;
		} else if (operation instanceof DropDatabaseOperation) {
			cmd = SqlCommand.DROP_DATABASE;
		} else if (operation instanceof AlterDatabaseOperation) {
			cmd = SqlCommand.ALTER_DATABASE;
		} else if (operation instanceof CreateCatalogOperation) {
			cmd = SqlCommand.CREATE_CATALOG;
		} else if (operation instanceof DropCatalogOperation) {
			cmd = SqlCommand.DROP_CATALOG;
		} else if (operation instanceof UseCatalogOperation) {
			cmd = SqlCommand.USE_CATALOG;
			operands = new String[]{((UseCatalogOperation) operation).getCatalogName()};
		} else if (operation instanceof UseDatabaseOperation) {
			cmd = SqlCommand.USE;
			operands = new String[]{((UseDatabaseOperation) operation).getDatabaseName()};
		} else if (operation instanceof ShowCatalogsOperation) {
			cmd = SqlCommand.SHOW_CATALOGS;
			operands = new String[0];
		} else if (operation instanceof ShowCurrentCatalogOperation) {
			cmd = SqlCommand.SHOW_CURRENT_CATALOG;
			operands = new String[0];
		} else if (operation instanceof ShowDatabasesOperation) {
			cmd = SqlCommand.SHOW_DATABASES;
			operands = new String[0];
		} else if (operation instanceof ShowCurrentDatabaseOperation) {
			cmd = SqlCommand.SHOW_CURRENT_DATABASE;
			operands = new String[0];
		} else if (operation instanceof ShowTablesOperation) {
			cmd = SqlCommand.SHOW_TABLES;
			operands = new String[0];
		} else if (operation instanceof ShowFunctionsOperation) {
			cmd = SqlCommand.SHOW_FUNCTIONS;
			operands = new String[0];
		} else if (operation instanceof CreateCatalogFunctionOperation ||
				operation instanceof CreateTempSystemFunctionOperation) {
			cmd = SqlCommand.CREATE_FUNCTION;
		} else if (operation instanceof DropCatalogFunctionOperation ||
				operation instanceof DropTempSystemFunctionOperation) {
			cmd = SqlCommand.DROP_FUNCTION;
		} else if (operation instanceof AlterCatalogFunctionOperation) {
			cmd = SqlCommand.ALTER_FUNCTION;
		} else if (operation instanceof ExplainOperation) {
			cmd = SqlCommand.EXPLAIN;
		} else if (operation instanceof DescribeTableOperation) {
			cmd = SqlCommand.DESCRIBE;
			operands = new String[]{((DescribeTableOperation) operation).getSqlIdentifier().asSerializableString()};
		} else if (operation instanceof QueryOperation) {
			cmd = SqlCommand.SELECT;
		} else {
			throw new Exception("Unknown operation: " + operation.asSummaryString());
		}

		return new SqlCommandCall(cmd, operands, stmt);
	}

	private static Optional<SqlCommandCall> parseByRegexMatching(String stmt) {
		// parse statement via regex matching
		for (SqlCommand cmd : SqlCommand.values()) {
			if (cmd.pattern != null) {
				final Matcher matcher = cmd.pattern.matcher(stmt);
				if (matcher.matches()) {
					final String[] groups = new String[matcher.groupCount()];
					for (int i = 0; i < groups.length; i++) {
						groups[i] = matcher.group(i + 1);
					}
					return cmd.operandConverter.apply(groups)
							.map((operands) -> {
								String[] newOperands = operands;
								if (cmd == SqlCommand.EXPLAIN) {
									// convert `explain xx` to `explain plan for xx`
									// which can execute through executeSql method
									newOperands = new String[]{"EXPLAIN PLAN FOR " + operands[0] + " " + operands[1]};
								}
								return new SqlCommandCall(cmd, newOperands, stmt);
							});
				}
			}
		}
		return Optional.empty();
	}


	public String sqlHelp() {
		return MESSAGE_HELP.toString();
	}


	public Map extractTableConfigOptions() {
		Map<String, ConfigOption> configOptions = new HashMap<>();
		configOptions.putAll(extractConfigOptions(ExecutionConfigOptions.class));
		configOptions.putAll(extractConfigOptions(OptimizerConfigOptions.class));
		configOptions.putAll(extractConfigOptions(TableConfigOptions.class));
		configOptions.putAll(extractConfigOptions(ExecutionCheckpointingOptions.class));
		return configOptions;
	}

	private Map<String, ConfigOption> extractConfigOptions(Class clazz) {
		Map<String, ConfigOption> configOptions = new HashMap();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			if (field.getType().isAssignableFrom(ConfigOption.class)) {
				try {
					ConfigOption configOption = (ConfigOption) field.get(ConfigOption.class);
					configOptions.put(configOption.key(), configOption);
				} catch (Throwable e) {
					LOGGER.warn("Fail to get ConfigOption", e);
				}
			}
		}
		return configOptions;
	}

	protected static AttributedString formatCommand(SqlCommand cmd, String description) {
		return new AttributedStringBuilder()
				.style(AttributedStyle.DEFAULT.bold())
				.append(cmd.toString())
				.append("\t\t")
				.style(AttributedStyle.DEFAULT)
				.append(description)
				.append('\n')
				.toAttributedString();
	}
}
