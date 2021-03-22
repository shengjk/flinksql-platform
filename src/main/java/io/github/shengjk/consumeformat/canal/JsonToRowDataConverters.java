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
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static io.github.shengjk.consumeformat.canal.TimeFormats.*;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

/**
 * Tool class used to convert from {@link JsonNode} to {@link RowData}. *
 */
@Internal
public class JsonToRowDataConverters implements Serializable {
	private final static Logger LOGGER = LoggerFactory.getLogger(JsonToRowDataConverters.class);

	private static final long serialVersionUID = 1L;

	/**
	 * Object mapper for parsing the JSON.
	 */
	private final ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * Flag indicating whether to fail if a field is missing.
	 */
	private final boolean failOnMissingField;

	/**
	 * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
	 */
	private final boolean ignoreParseErrors;

	/**
	 * Timestamp format specification which is used to parse timestamp.
	 */
	private final TimestampFormat timestampFormat;

	public JsonToRowDataConverters(
			boolean failOnMissingField,
			boolean ignoreParseErrors,
			TimestampFormat timestampFormat) {
		this.failOnMissingField = failOnMissingField;
		this.ignoreParseErrors = ignoreParseErrors;
		this.timestampFormat = timestampFormat;
	}

	/**
	 * Runtime converter that converts {@link JsonNode}s into objects of Flink Table & SQL internal
	 * data structures.
	 */
	@FunctionalInterface
	public interface JsonToRowDataConverter extends Serializable {
		Object convert(JsonNode jsonNode);
	}

	/**
	 * Creates a runtime converter which is null safe.
	 */
	public JsonToRowDataConverter createConverter(LogicalType type) {
		return wrapIntoNullableConverter(createNotNullConverter(type));
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private JsonToRowDataConverter createNotNullConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return null;
					}
				};
			case BOOLEAN:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToBoolean(jsonNode);
					}
				};
			case TINYINT:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return Byte.parseByte(jsonNode.asText().trim());
					}
				};
			case SMALLINT:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return Short.parseShort(jsonNode.asText().trim());
					}
				};
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToInt(jsonNode);
					}
				};
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToLong(jsonNode);
					}
				};
			case DATE:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToDate(jsonNode);
					}
				};
			case TIME_WITHOUT_TIME_ZONE:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToTime(jsonNode);
					}
				};
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToTimestamp(jsonNode);
					}
				};
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToTimestampWithLocalZone(jsonNode);
					}
				};
			case FLOAT:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToFloat(jsonNode);
					}
				};
			case DOUBLE:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToDouble(jsonNode);
					}
				};
			case CHAR:
			case VARCHAR:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToString(jsonNode);
					}
				};
			case BINARY:
			case VARBINARY:
				return new JsonToRowDataConverter() {
					@Override
					public Object convert(JsonNode jsonNode) {
						return convertToBytes(jsonNode);
					}
				};
			case DECIMAL:
				return createDecimalConverter((DecimalType) type);
			case ARRAY:
				return createArrayConverter((ArrayType) type);
			case MAP:
				MapType mapType = (MapType) type;
				return createMapConverter(
						mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
			case MULTISET:
				MultisetType multisetType = (MultisetType) type;
				return createMapConverter(
						multisetType.asSummaryString(),
						multisetType.getElementType(),
						new IntType());
			case ROW:
				return createRowConverter((RowType) type);
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	private boolean convertToBoolean(JsonNode jsonNode) {
		if (jsonNode.isBoolean()) {
			// avoid redundant toString and parseBoolean, for better performance
			return jsonNode.asBoolean();
		} else {
			return Boolean.parseBoolean(jsonNode.asText().trim());
		}
	}

	private int convertToInt(JsonNode jsonNode) {
		if (jsonNode.canConvertToInt()) {
			// avoid redundant toString and parseInt, for better performance
			return jsonNode.asInt();
		} else {
			return Integer.parseInt(jsonNode.asText().trim());
		}
	}

	private long convertToLong(JsonNode jsonNode) {
		if (jsonNode.canConvertToLong()) {
			// avoid redundant toString and parseLong, for better performance
			return jsonNode.asLong();
		} else {
			return Long.parseLong(jsonNode.asText().trim());
		}
	}

	private double convertToDouble(JsonNode jsonNode) {
		if (jsonNode.isDouble()) {
			// avoid redundant toString and parseDouble, for better performance
			return jsonNode.asDouble();
		} else {
			return Double.parseDouble(jsonNode.asText().trim());
		}
	}

	private float convertToFloat(JsonNode jsonNode) {
		if (jsonNode.isDouble()) {
			// avoid redundant toString and parseDouble, for better performance
			return (float) jsonNode.asDouble();
		} else {
			return Float.parseFloat(jsonNode.asText().trim());
		}
	}

	private int convertToDate(JsonNode jsonNode) {
		LocalDate date = ISO_LOCAL_DATE.parse(jsonNode.asText()).query(TemporalQueries.localDate());
		return (int) date.toEpochDay();
	}

	private int convertToTime(JsonNode jsonNode) {
		TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(jsonNode.asText());
		LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

		// get number of milliseconds of the day
		return localTime.toSecondOfDay() * 1000;
	}

	private TimestampData convertToTimestamp(JsonNode jsonNode) {
		TemporalAccessor parsedTimestamp;
		switch (timestampFormat) {
			case SQL:
				parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(jsonNode.asText());
				break;
			case ISO_8601:
				parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(jsonNode.asText());
				break;
			default:
				throw new TableException(
						String.format(
								"Unsupported timestamp format '%s'. Validator should have checked that.",
								timestampFormat));
		}
		LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
		LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

		return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
	}

	private TimestampData convertToTimestampWithLocalZone(JsonNode jsonNode) {
		TemporalAccessor parsedTimestampWithLocalZone;
		switch (timestampFormat) {
			case SQL:
				parsedTimestampWithLocalZone =
						SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(jsonNode.asText());
				break;
			case ISO_8601:
				parsedTimestampWithLocalZone =
						ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(jsonNode.asText());
				break;
			default:
				throw new TableException(
						String.format(
								"Unsupported timestamp format '%s'. Validator should have checked that.",
								timestampFormat));
		}
		LocalTime localTime = parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
		LocalDate localDate = parsedTimestampWithLocalZone.query(TemporalQueries.localDate());

		return TimestampData.fromInstant(
				LocalDateTime.of(localDate, localTime).toInstant(ZoneOffset.UTC));
	}

	private StringData convertToString(JsonNode jsonNode) {
		if (jsonNode.isContainerNode()) {
			return StringData.fromString(jsonNode.toString());
		} else {
			return StringData.fromString(jsonNode.asText());
		}
	}

	private byte[] convertToBytes(JsonNode jsonNode) {
		try {
			return jsonNode.binaryValue();
		} catch (IOException e) {
			throw new JsonParseException("Unable to deserialize byte array.", e);
		}
	}

	private JsonToRowDataConverter createDecimalConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return new JsonToRowDataConverter() {
			@Override
			public Object convert(JsonNode jsonNode) {
				BigDecimal bigDecimal;
				if (jsonNode.isBigDecimal()) {
					bigDecimal = jsonNode.decimalValue();
				} else {
					bigDecimal = new BigDecimal(jsonNode.asText());
				}
				return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
			}
		};
	}

	private JsonToRowDataConverter createArrayConverter(ArrayType arrayType) {
		JsonToRowDataConverter elementConverter = createConverter(arrayType.getElementType());
		final Class<?> elementClass =
				LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
		return new JsonToRowDataConverter() {
			@Override
			public Object convert(JsonNode jsonNode) {
				final ArrayNode node = (ArrayNode) jsonNode;
				final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
				for (int i = 0; i < node.size(); i++) {
					final JsonNode innerNode = node.get(i);
					array[i] = elementConverter.convert(innerNode);
				}
				return new GenericArrayData(array);
			}
		};
	}

	private JsonToRowDataConverter createMapConverter(
			String typeSummary, LogicalType keyType, LogicalType valueType) {
		if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
			throw new UnsupportedOperationException(
					"JSON format doesn't support non-string as key type of map. "
							+ "The type is: "
							+ typeSummary);
		}
		final JsonToRowDataConverter keyConverter = createConverter(keyType);
		final JsonToRowDataConverter valueConverter = createConverter(valueType);
		return new JsonToRowDataConverter() {
			@Override
			public Object convert(JsonNode jsonNode) {
//				LOGGER.info("begin map converter");
				Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
				Map<Object, Object> result = new HashMap<>();
				while (fields.hasNext()) {
					Map.Entry<String, JsonNode> entry = fields.next();
					Object key = keyConverter.convert(TextNode.valueOf(entry.getKey()));
					Object value = valueConverter.convert(entry.getValue());
					result.put(key, value);
				}
//				LOGGER.info("end map converter");
				return new GenericMapData(result);
			}
		};
	}

	public JsonToRowDataConverter createRowConverter(RowType rowType) {
		final JsonToRowDataConverter[] fieldConverters =
				rowType.getFields().stream()
						.map(RowType.RowField::getType)
						.map(this::createConverter)
						.toArray(JsonToRowDataConverter[]::new);
		final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

		return new JsonToRowDataConverter() {
			@Override
			public Object convert(JsonNode jsonNode) {
				if (jsonNode instanceof TextNode) {
					String s = jsonNode.textValue();
					try {
						jsonNode = objectMapper.readTree(s);
					} catch (JsonProcessingException e) {
						e.printStackTrace();
					}
				}

				ObjectNode node = (ObjectNode) jsonNode;
				int arity = fieldNames.length;
				GenericRowData row = new GenericRowData(arity);
				for (int i = 0; i < arity; i++) {
					String fieldName = fieldNames[i];
					JsonNode field = node.get(fieldName);
					Object convertedField = convertField(fieldConverters[i], fieldName, field);
					row.setField(i, convertedField);
				}
				return row;
			}
		};
	}


	//就目前的json格式来说，只有 ObjectJsonNode TextJsonNode
	private Object convertField(
			JsonToRowDataConverter fieldConverter,
			String fieldName,
			JsonNode field) {
//		LOGGER.info(" convertField======= begin");
		if (field == null) {
			if (failOnMissingField) {
				throw new JsonParseException(
						"Could not find field with name '" + fieldName + "'.");
			} else {
				return null;
			}
		} else if ((field instanceof ObjectNode) && field.isEmpty()) {
			return null;
		} else if ((field instanceof TextNode) && field.asText().trim().length() == 0) {
			return null;
		} else if (field instanceof BooleanNode) {
			try {
				field.asBoolean();
				return fieldConverter.convert(field);
			} catch (Exception e) {
				return null;
			}
		} else if (field instanceof ArrayNode && field.isEmpty()) {
			return null;
		} else if (field instanceof NumericNode) {
			try {
				Long.parseLong(field.asText());
				return fieldConverter.convert(field);
			} catch (Exception e) {
				return null;
			}
			// BinaryNode  NullNode POJONode
		} else if ((field instanceof BinaryNode || field instanceof NullNode || field instanceof POJONode) && (field.asText().trim().length() == 0 || "null".equals(field.asText().trim()))) {
			return null;
		} else {
			return fieldConverter.convert(field);
		}
	}

	private JsonToRowDataConverter wrapIntoNullableConverter(JsonToRowDataConverter converter) {
		return new JsonToRowDataConverter() {
			@Override
			public Object convert(JsonNode jsonNode) {
				if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
					return null;
				} else if ((jsonNode instanceof ObjectNode) && jsonNode.isEmpty()) {
					return null;
				} else if ((jsonNode instanceof TextNode) && jsonNode.asText().trim().length() == 0) {
					return null;
				} else if (jsonNode instanceof BooleanNode) {
					try {
						jsonNode.asBoolean();
					} catch (Exception e) {
						return null;
					}
				} else if (jsonNode instanceof ArrayNode && jsonNode.isEmpty()) {
					return null;
				} else if (jsonNode instanceof NumericNode) {
					try {
						Long.parseLong(jsonNode.asText());
					} catch (Exception e) {
						return null;
					}
					// BinaryNode  NullNode POJONode
				} else if ((jsonNode instanceof BinaryNode || jsonNode instanceof NullNode || jsonNode instanceof POJONode) &&
						(jsonNode.asText().trim().length() == 0 || "null".equals(jsonNode.asText().trim()))) {
					return null;
				}
				try {
					return converter.convert(jsonNode);
				} catch (Throwable t) {
					if (!ignoreParseErrors) {
						throw t;
					}
					return null;
				}
			}
		};
	}

	/**
	 * Exception which refers to parse errors in converters.
	 */
	private static final class JsonParseException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public JsonParseException(String message) {
			super(message);
		}

		public JsonParseException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
