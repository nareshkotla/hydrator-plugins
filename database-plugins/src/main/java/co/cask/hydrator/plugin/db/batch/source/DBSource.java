/*
 * Copyright © 2015-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.db.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.plugin.DBConfig;
import co.cask.hydrator.plugin.DBManager;
import co.cask.hydrator.plugin.DBRecord;
import co.cask.hydrator.plugin.DBUtils;
import co.cask.hydrator.plugin.DriverCleanup;
import co.cask.hydrator.plugin.FieldCase;
import co.cask.hydrator.plugin.StructuredRecordUtils;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import javax.annotation.Nullable;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Path;

/**
 * Batch source to read from a DB table
 */
@Plugin(type = "batchsource")
@Name("Database")
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class DBSource extends BatchSource<LongWritable, DBRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DBSource.class);

  private final DBSourceConfig sourceConfig;
  private final DBManager dbManager;
  private Class<? extends Driver> driverClass;

  public DBSource(DBSourceConfig sourceConfig) {
    this.sourceConfig = sourceConfig;
    this.dbManager = new DBManager(sourceConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    dbManager.validateJDBCPluginPipeline(pipelineConfigurer, getJDBCPluginId());
    Preconditions.checkArgument(sourceConfig.getImportQuery().contains("$CONDITIONS"),
                                "Import Query %s must contain the string '$CONDITIONS'.",
                                sourceConfig.importQuery);
  }

  class GetSchemaRequest {
    public String connectionString;
    @Nullable
    public String user;
    @Nullable
    public String password;
    public String jdbcPluginName;
    @Nullable
    public String jdbcPluginType = "jdbc";
    public String query;
  }

  /**
   * Endpoint method to get the output schema of a query.
   *
   * @param request {@link GetSchemaRequest} containing information required for connection and query to execute.
   * @param pluginContext context to create plugins
   * @return schema of fields
   * @throws SQLException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws BadRequestException If executing the query threw {@link SQLSyntaxErrorException}
   * we get the message and throw it as BadRequestException
   */
  @Path("getSchema")
  public Schema getSchema(GetSchemaRequest request,
                          EndpointPluginContext pluginContext)
    throws SQLException, InstantiationException, IllegalAccessException, BadRequestException {
    DriverCleanup driverCleanup = loadPluginClassAndGetDriver(request, pluginContext);
    try {
      try (Connection connection = getConnection(request.connectionString, request.user, request.password)) {
        Statement statement = connection.createStatement();
        statement.setMaxRows(1);
        ResultSet resultSet = statement.executeQuery(request.query);
        return Schema.recordOf("outputSchema", DBUtils.getSchemaFields(resultSet));
      }
    } catch (SQLSyntaxErrorException e) {
      throw new BadRequestException(e.getMessage());
    } finally {
      driverCleanup.destroy();
    }
  }

  private DriverCleanup loadPluginClassAndGetDriver(GetSchemaRequest request, EndpointPluginContext pluginContext)
    throws IllegalAccessException, InstantiationException, SQLException {
    Class<? extends Driver> driverClass =
      pluginContext.loadPluginClass(request.jdbcPluginType, request.jdbcPluginName, PluginProperties.builder().build());

    try {
      return DBUtils.ensureJDBCDriverIsAvailable(driverClass, request.connectionString,
                                                 request.jdbcPluginType, request.jdbcPluginName);
    } catch (IllegalAccessException | InstantiationException | SQLException e) {
      LOG.error("Unable to load or register driver {}", driverClass, e);
      throw e;
    }
  }

  private Connection getConnection(String connectionString,
                                   @Nullable String user, @Nullable String password) throws SQLException {
    if (user == null) {
      return DriverManager.getConnection(connectionString);
    } else {
      return DriverManager.getConnection(connectionString, user, password);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    LOG.debug("pluginType = {}; pluginName = {}; connectionString = {}; importQuery = {}; " +
                "boundingQuery = {}",
              sourceConfig.jdbcPluginType, sourceConfig.jdbcPluginName,
              sourceConfig.connectionString, sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery());

    Job job = Job.getInstance();
    Configuration hConf = job.getConfiguration();
    hConf.clear();

    // Load the plugin class to make sure it is available.
    Class<? extends Driver> driverClass = context.loadPluginClass(getJDBCPluginId());
    if (sourceConfig.user == null && sourceConfig.password == null) {
      DBConfiguration.configureDB(hConf, driverClass.getName(), sourceConfig.connectionString);
    } else {
      DBConfiguration.configureDB(hConf, driverClass.getName(), sourceConfig.connectionString,
                                  sourceConfig.user, sourceConfig.password);
    }
    DataDrivenETLDBInputFormat.setInput(hConf, DBRecord.class,
                                        sourceConfig.getImportQuery(), sourceConfig.getBoundingQuery(),
                                        sourceConfig.getEnableAutoCommit());
    hConf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, sourceConfig.splitBy);
    context.setInput(new SourceInputFormatProvider(DataDrivenETLDBInputFormat.class, hConf));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    driverClass = context.loadPluginClass(getJDBCPluginId());
  }

  @Override
  public void transform(KeyValue<LongWritable, DBRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecordUtils.convertCase(
      input.getValue().getRecord(), FieldCase.toFieldCase(sourceConfig.columnNameCase)));
  }

  @Override
  public void destroy() {
    try {
      DBUtils.cleanup(driverClass);
    } finally {
      dbManager.destroy();
    }
  }

  private String getJDBCPluginId() {
    return String.format("%s.%s.%s", "source", sourceConfig.jdbcPluginType, sourceConfig.jdbcPluginName);
  }

  /**
   * {@link PluginConfig} for {@link DBSource}
   */
  public static class DBSourceConfig extends DBConfig {
    public static final String BOUNDING_QUERY = "boundingQuery";
    public static final String SPLIT_BY = "splitBy";

    @Description("The SELECT query to use to import data from the specified table. " +
      "You can specify an arbitrary number of columns to import, or import all columns using *. " +
      "The Query should contain the '$CONDITIONS' string. " +
      "For example, 'SELECT * FROM table WHERE $CONDITIONS'. The '$CONDITIONS' string" +
      "will be replaced by 'splitBy' field limits specified by the bounding query.")
    String importQuery;

    @Name(BOUNDING_QUERY)
    @Description("Bounding Query should return the min and max of the " +
      "values of the 'splitBy' field. For example, 'SELECT MIN(id),MAX(id) FROM table'")
    String boundingQuery;

    @Name(SPLIT_BY)
    @Description("Field Name which will be used to generate splits.")
    String splitBy;

    private String getImportQuery() {
      return cleanQuery(importQuery);
    }

    private String getBoundingQuery() {
      return cleanQuery(boundingQuery);
    }
  }
}