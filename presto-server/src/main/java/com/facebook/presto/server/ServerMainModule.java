/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.event.query.QueryCompletionEvent;
import com.facebook.presto.event.query.QueryCreatedEvent;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.CreateOrReplaceMaterializedViewExecution.CreateOrReplaceMaterializedViewExecutionFactory;
import com.facebook.presto.execution.FailureInfo;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.Sitevars;
import com.facebook.presto.execution.SitevarsConfig;
import com.facebook.presto.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.execution.SqlTaskManager;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.importer.ForImportManager;
import com.facebook.presto.importer.ForPeriodicImport;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.importer.ImportManagerConfig;
import com.facebook.presto.importer.JobStateFactory;
import com.facebook.presto.importer.LocalShardManager;
import com.facebook.presto.importer.NodeWorkerQueue;
import com.facebook.presto.importer.PeriodicImportConfig;
import com.facebook.presto.importer.PeriodicImportController;
import com.facebook.presto.importer.PeriodicImportJobResource;
import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.importer.PeriodicImportRunnable;
import com.facebook.presto.importer.ShardImport;
import com.facebook.presto.metadata.DatabaseShardManager;
import com.facebook.presto.metadata.DatabaseStorageManager;
import com.facebook.presto.metadata.ForMetadata;
import com.facebook.presto.metadata.ForShardManager;
import com.facebook.presto.metadata.ForStorageManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.ImportMetadata;
import com.facebook.presto.metadata.InformationSchemaData;
import com.facebook.presto.metadata.InformationSchemaMetadata;
import com.facebook.presto.metadata.InternalMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataResource;
import com.facebook.presto.metadata.NativeMetadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.metadata.StorageManagerConfig;
import com.facebook.presto.metadata.SystemTables;
import com.facebook.presto.operator.ForExchange;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.ImportClientManager;
import com.facebook.presto.split.ImportDataStreamProvider;
import com.facebook.presto.split.InternalDataStreamProvider;
import com.facebook.presto.split.NativeDataStreamProvider;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.tree.CreateOrReplaceMaterializedView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Serialization.ExpressionDeserializer;
import com.facebook.presto.sql.tree.Serialization.ExpressionSerializer;
import com.facebook.presto.sql.tree.Serialization.FunctionCallDeserializer;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.Statement;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import io.airlift.dbpool.H2EmbeddedDataSourceModule;
import io.airlift.dbpool.MySqlDataSourceModule;
import io.airlift.discovery.client.ServiceAnnouncement.ServiceAnnouncementBuilder;
import io.airlift.http.client.HttpClientBinder;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.weakref.jmx.guice.ExportBinder;

import javax.inject.Singleton;

import java.io.File;
import java.lang.annotation.Annotation;

import static com.facebook.presto.server.ConditionalModule.installIfPropertyEquals;
import static com.facebook.presto.server.DbiProvider.bindDbiToDataSource;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.event.client.EventBinder.eventBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ServerMainModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void configure()
    {
        httpServerBinder(binder).bindResource("/", "webapp").withWelcomeFile("index.html");

        binder.bind(QueryResource.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(QueryManagerConfig.class);

        binder.bind(TaskResource.class).in(Scopes.SINGLETON);
        binder.bind(TaskManager.class).to(SqlTaskManager.class).in(Scopes.SINGLETON);
        binder.bind(ExchangeOperatorFactory.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);

        binder.bind(PagesMapper.class).in(Scopes.SINGLETON);
        binder.bind(LocationFactory.class).to(HttpLocationFactory.class).in(Scopes.SINGLETON);
        binder.bind(RemoteTaskFactory.class).to(HttpRemoteTaskFactory.class).in(Scopes.SINGLETON);

        HttpClientBinder.httpClientBinder(binder).bindAsyncHttpClient("exchange", ForExchange.class).withTracing();
        HttpClientBinder.httpClientBinder(binder).bindAsyncHttpClient("scheduler", ForScheduler.class).withTracing();

        bindConfig(binder).to(StorageManagerConfig.class);
        binder.bind(StorageManager.class).to(DatabaseStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(DataStreamProvider.class).to(DataStreamManager.class).in(Scopes.SINGLETON);
        binder.bind(NativeDataStreamProvider.class).in(Scopes.SINGLETON);
        binder.bind(ImportDataStreamProvider.class).in(Scopes.SINGLETON);

        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(NativeMetadata.class).in(Scopes.SINGLETON);

        binder.bind(MetadataResource.class).in(Scopes.SINGLETON);

        binder.bind(InternalMetadata.class).in(Scopes.SINGLETON);
        binder.bind(InternalDataStreamProvider.class).in(Scopes.SINGLETON);
        binder.bind(InformationSchemaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(InformationSchemaData.class).in(Scopes.SINGLETON);
        binder.bind(SystemTables.class).in(Scopes.SINGLETON);

        binder.bind(ImportClientManager.class).in(Scopes.SINGLETON);

        // kick off binding of import client factories
        Multibinder.newSetBinder(binder, ImportClientFactory.class);
        binder.bind(ImportMetadata.class).in(Scopes.SINGLETON);

        binder.bind(SplitManager.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(QueryFragmentRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(Split.class);
        jsonBinder(binder).addSerializerBinding(Expression.class).to(ExpressionSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Expression.class).to(ExpressionDeserializer.class);
        jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(FunctionCallDeserializer.class);

        jsonCodecBinder(binder).bindJsonCodec(StageInfo.class);
        jsonCodecBinder(binder).bindListJsonCodec(FailureInfo.class);
        binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);
        eventBinder(binder).bindEventClient(QueryCreatedEvent.class);
        eventBinder(binder).bindEventClient(QueryCompletionEvent.class);

        discoveryBinder(binder).bindSelector("presto");

        binder.bind(NodeManager.class).in(Scopes.SINGLETON);
        binder.bind(NodeWorkerQueue.class).in(Scopes.SINGLETON);
        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);

        binder.bind(ImportManager.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(ImportManagerConfig.class);
        httpClientBinder(binder).bindHttpClient("importer", ForImportManager.class).withFilter(NodeIdUserAgentRequestFilter.class);

        binder.bind(LocalShardManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardResource.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(ShardImport.class);

        ServiceAnnouncementBuilder announcementBuilder = discoveryBinder(binder).bindHttpAnnouncement("presto");
        String datasources = configurationFactory.getProperties().get("datasources");
        if (datasources != null) {
            configurationFactory.consumeProperty("datasources");
            announcementBuilder.addProperty("datasources", datasources);
        }

        String coordinatorProperty = configurationFactory.getProperties().get("coordinator");
        if (coordinatorProperty != null) {
            configurationFactory.consumeProperty("coordinator");
        }
        // default coordinator value is true
        if (coordinatorProperty == null || Boolean.parseBoolean(coordinatorProperty)) {
            discoveryBinder(binder).bindHttpAnnouncement("presto-coordinator");
        }

        bindDataSource("presto-metastore", ForMetadata.class, ForShardManager.class, ForPeriodicImport.class);

        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        binder.bind(ExecuteResource.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindAsyncHttpClient("execute", ExecuteResource.ForExecute.class);

        binder.install(new HandleJsonModule());

        binder.bind(PluginManager.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(PluginManagerConfig.class);

        // Job Scheduler code
        bindConfig(binder).to(PeriodicImportConfig.class);
        binder.bind(PeriodicImportJobResource.class).in(Scopes.SINGLETON);
        binder.bind(PeriodicImportManager.class).in(Scopes.SINGLETON);
        binder.bind(PeriodicImportController.class).in(Scopes.SINGLETON);
        binder.bind(JobStateFactory.class).in(Scopes.SINGLETON);
        binder.bind(PeriodicImportRunnable.PeriodicImportRunnableFactory.class).in(Scopes.SINGLETON);
        ExportBinder.newExporter(binder).export(PeriodicImportController.class).as("com.facebook.presto:name=periodic-import");

        bindConfig(binder).to(SitevarsConfig.class);
        binder.bind(Sitevars.class).in(Scopes.SINGLETON);
        ExportBinder.newExporter(binder).export(Sitevars.class).as("com.facebook.presto:name=sitevars");

        binder.bind(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);

        MapBinder<Class<? extends Statement>, QueryExecutionFactory<?>> executionBinder = MapBinder.newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {},
                new TypeLiteral<QueryExecutionFactory<?>>() {});
        executionBinder.addBinding(CreateOrReplaceMaterializedView.class).to(CreateOrReplaceMaterializedViewExecutionFactory.class).in(Scopes.SINGLETON);

        binder.bind(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(Query.class).to(Key.get(SqlQueryExecutionFactory.class)).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowColumns.class).to(Key.get(SqlQueryExecutionFactory.class)).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowPartitions.class).to(Key.get(SqlQueryExecutionFactory.class)).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowFunctions.class).to(Key.get(SqlQueryExecutionFactory.class)).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowTables.class).to(Key.get(SqlQueryExecutionFactory.class)).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForStorageManager
    public IDBI createStorageManagerDBI(StorageManagerConfig config)
            throws Exception
    {
        String path = new File(config.getDataDirectory(), "db/StorageManager").getAbsolutePath();
        return new DBI(new H2EmbeddedDataSource(new H2EmbeddedDataSourceConfig().setFilename(path).setMaxConnections(500).setMaxConnectionWait(new Duration(1, SECONDS))));
    }

    @SafeVarargs
    private final void bindDataSource(String type, Class<? extends Annotation> annotation, Class<? extends Annotation>... aliases)
    {
        String property = type + ".db.type";
        install(installIfPropertyEquals(new MySqlDataSourceModule(type, annotation, aliases), property, "mysql"));
        install(installIfPropertyEquals(new H2EmbeddedDataSourceModule(type, annotation, aliases), property, "h2"));

        bindDbiToDataSource(binder, annotation);
        for (Class<? extends Annotation> alias : aliases) {
            bindDbiToDataSource(binder, alias);
        }
    }
}
