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

package org.apache.druid.server.vectorizedlookup.jdbc;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.server.vectorizedlookup.DataFetcher;
import org.apache.druid.utils.ConnectionUriUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class JdbcDataFetcher implements DataFetcher<String, String>
{
  private static final Logger LOGGER = new Logger(JdbcDataFetcher.class);

  /**
   * Escape a SQL identifier (table or column name) to prevent SQL injection.
   * This is a simple implementation that wraps the identifier in double quotes.
   * For production use, consider using a more robust SQL escaping library.
   */
  private String escapeIdentifier(String identifier) {
    if (identifier == null || identifier.trim().isEmpty()) {
      throw new IllegalArgumentException("Identifier cannot be null or empty");
    }
    // Simple escaping: wrap in double quotes and escape any existing quotes
    return "\"" + StringUtils.replace(identifier, "\"", "\"\"") + "\"";
  }

  private String escapeSqlValue(String value) {
    if (value == null) {
      return "NULL";
    }
    // Escape single quotes by doubling them
    return StringUtils.replace(value, "'", "''");
  }
  private static final int DEFAULT_STREAMING_FETCH_SIZE = 1000;

  @JsonProperty
  private final MetadataStorageConnectorConfig connectorConfig;
  @JsonProperty
  private final String table;
  @JsonProperty
  private final String keyColumn;
  @JsonProperty
  private final String valueColumn;
  @JsonProperty
  private final int streamingFetchSize;

  private final String fetchAllQuery;
  private final String fetchQuery;
  private final String reverseFetchQuery;
  private final DBI dbi;

  JdbcDataFetcher(
      @JsonProperty("connectorConfig") MetadataStorageConnectorConfig connectorConfig,
      @JsonProperty("table") String table,
      @JsonProperty("keyColumn") String keyColumn,
      @JsonProperty("valueColumn") String valueColumn,
      @JsonProperty("streamingFetchSize") @Nullable Integer streamingFetchSize,
      @JacksonInject JdbcAccessSecurityConfig securityConfig
  )
  {
    this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
    this.streamingFetchSize = streamingFetchSize == null ? DEFAULT_STREAMING_FETCH_SIZE : streamingFetchSize;
    // Check the properties in the connection URL. Note that JdbcDataFetcher doesn't use
    // MetadataStorageConnectorConfig.getDbcpProperties(). If we want to use them,
    // those DBCP properties should be validated using the same logic.
    checkConnectionURL(connectorConfig.getConnectURI(), securityConfig);
    this.table = Preconditions.checkNotNull(table, "table");
    this.keyColumn = Preconditions.checkNotNull(keyColumn, "keyColumn");
    this.valueColumn = Preconditions.checkNotNull(valueColumn, "valueColumn");

    this.fetchAllQuery = StringUtils.format(
        "SELECT %s, %s FROM %s",
        this.keyColumn,
        this.valueColumn,
        this.table
    );
    this.fetchQuery = StringUtils.format(
        "SELECT %s FROM %s WHERE %s = :val",
        this.valueColumn,
        this.table,
        this.keyColumn
    );
    this.reverseFetchQuery = StringUtils.format(
        "SELECT %s FROM %s WHERE %s = :val",
        this.keyColumn,
        this.table,
        this.valueColumn
    );
    dbi = new DBI(
        connectorConfig.getConnectURI(),
        connectorConfig.getUser(),
        connectorConfig.getPassword()
    );
    LOGGER.info("DBI object created successfully for table [%s]", table);

    dbi.registerMapper(new KeyValueResultSetMapper(keyColumn, valueColumn));
  }

  /**
   * Check the given URL whether it contains non-allowed properties.
   *
   * @see JdbcAccessSecurityConfig#getAllowedProperties()
   * @see ConnectionUriUtils#tryParseJdbcUriParameters(String, boolean)
   */
  private static void checkConnectionURL(String url, JdbcAccessSecurityConfig securityConfig)
  {
    Preconditions.checkNotNull(url, "connectorConfig.connectURI");

    if (!securityConfig.isEnforceAllowedProperties()) {
      // You don't want to do anything with properties.
      return;
    }

    ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
        ConnectionUriUtils.tryParseJdbcUriParameters(url, securityConfig.isAllowUnknownJdbcUrlFormat()),
        securityConfig.getSystemPropertyPrefixes(),
        securityConfig.getAllowedProperties()
    );
  }

  @Override
  public Iterable<Map.Entry<String, String>> fetchAll()
  {
    LOGGER.info("Fetching all key-value pairs from table [%s]", table);
    return inReadOnlyTransaction((handle, status) -> handle.createQuery(fetchAllQuery)
                                                           .setFetchSize(streamingFetchSize)
                                                           .map(new KeyValueResultSetMapper(keyColumn, valueColumn))
                                                           .list());
  }

  @Override
  public String fetch(final String key)
  {
    LOGGER.info("Fetching value for key [%s] from table [%s]", key, table);
    List<String> pairs = inReadOnlyTransaction(
        (handle, status) -> handle.createQuery(fetchQuery)
                                  .bind("val", key)
                                  .map(StringMapper.FIRST)
                                  .list()
    );
    if (pairs.isEmpty()) {
      LOGGER.info("No value found for key [%s] in table [%s]", key, table);
      return null;
    }
    LOGGER.info("Found value for key [%s] in table [%s]", key, table);
    return pairs.get(0);
  }

  @Override
  public Iterable<Map.Entry<String, String>> fetch(final Iterable<String> keys)
  {
    return runWithMissingJdbcJarHandler(
        () -> {
          // Convert Iterable to List for easier handling
          List<String> keysList = Lists.newArrayList(keys);

          // Build SQL with keys directly embedded and properly escaped
          StringBuilder inClause = new StringBuilder();
          for (int i = 0; i < keysList.size(); i++) {
            if (i > 0) inClause.append(", ");
            // Escape the key value for SQL injection protection
            inClause.append("'").append(escapeSqlValue(keysList.get(i))).append("'");
          }

          String sql = StringUtils.format(
              "SELECT %s, %s FROM %s WHERE %s IN (%s)",
              escapeIdentifier(keyColumn),
              escapeIdentifier(valueColumn),
              escapeIdentifier(table),
              escapeIdentifier(keyColumn),
              inClause.toString()
          );

          // Execute the SQL using the connection pool like other methods
          return inReadOnlyTransaction((handle, status) -> handle.createQuery(sql)
              .map(new KeyValueResultSetMapper(keyColumn, valueColumn))
              .list());
        }
    );
  }

  @Override
  public List<String> reverseFetchKeys(final String value)
  {
    LOGGER.info("Reverse fetching keys for value [%s] from table [%s]", value, table);
    return inReadOnlyTransaction((handle, status) -> handle.createQuery(reverseFetchQuery)
                                                           .bind("val", value)
                                                           .map(StringMapper.FIRST)
                                                           .list());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JdbcDataFetcher)) {
      return false;
    }

    JdbcDataFetcher that = (JdbcDataFetcher) o;

    if (!connectorConfig.equals(that.connectorConfig)) {
      return false;
    }
    if (!table.equals(that.table)) {
      return false;
    }
    if (!keyColumn.equals(that.keyColumn)) {
      return false;
    }
    return valueColumn.equals(that.valueColumn);

  }

  @Override
  public int hashCode()
  {
    return Objects.hash(connectorConfig, table, keyColumn, valueColumn);
  }

  @Override
  public String toString()
  {
    return "JdbcDataFetcher{" +
           "table='" + table + '\'' +
           ", keyColumn='" + keyColumn + '\'' +
           ", valueColumn='" + valueColumn + '\'' +
           '}';
  }

  private DBI getDbi()
  {
    return dbi;
  }

  private <T> T inReadOnlyTransaction(final TransactionCallback<T> callback)
  {
    return runWithMissingJdbcJarHandler(
        () ->
            getDbi().withHandle(
                handle -> {
                  final Connection connection = handle.getConnection();
                  final boolean readOnly = connection.isReadOnly();
                  connection.setReadOnly(true);
                  try {
                    return handle.inTransaction(callback);
                  }
                  finally {
                    try {
                      connection.setReadOnly(readOnly);
                    }
                    catch (SQLException e) {
                      // at least try to log it so we don't swallow exceptions
                      LOGGER.error(e, "Unable to reset connection read-only state");
                    }
                  }
                }
            )
    );
  }

  private <T> T runWithMissingJdbcJarHandler(Supplier<T> supplier)
  {
    try {
      return supplier.get();
    }
    catch (UnableToObtainConnectionException e) {
      if (e.getMessage().contains("No suitable driver found")) {
        throw new ISE(
            e,
            "JDBC driver JAR files missing in the classpath"
        );
      } else {
        throw e;
      }
    }
  }
}
