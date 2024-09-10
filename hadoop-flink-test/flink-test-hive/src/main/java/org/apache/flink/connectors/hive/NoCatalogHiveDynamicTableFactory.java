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

package org.apache.flink.connectors.hive;

import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_PARTITION_INCLUDE;
import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_READ_PARTITION_WITH_SUBDIRECTORY_ENABLED;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.connector.file.table.FileSystemTableFactory;
import org.apache.flink.connectors.hive.util.JobConfUtils;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/** A dynamic table factory implementation for Hive. */
public class NoCatalogHiveDynamicTableFactory extends FileSystemTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String IDENTIFIER = "hive";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(
                        NoCatalogHiveOptions.METASTORE_THRIFT_URIS,
                        NoCatalogHiveOptions.DATABASE,
                        NoCatalogHiveOptions.TABLE_NAME,
                        FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = super.optionalOptions();
        optionalOptions.addAll(
                Stream.of(
                                HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER,
                                HiveOptions
                                        .TABLE_EXEC_HIVE_READ_PARTITION_WITH_SUBDIRECTORY_ENABLED,
                                HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MODE,
                                HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX,
                                HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER,
                                HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM,
                                HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES,
                                HiveOptions.TABLE_EXEC_HIVE_FILE_OPEN_COST,
                                HiveOptions.TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM,
                                HiveOptions.TABLE_EXEC_HIVE_DYNAMIC_GROUPING_ENABLED,
                                HiveOptions.TABLE_EXEC_HIVE_READ_STATISTICS_THREAD_NUM,
                                HiveOptions.COMPACT_SMALL_FILES_AVG_SIZE,
                                HiveOptions.TABLE_EXEC_HIVE_SINK_STATISTIC_AUTO_GATHER_ENABLE,
                                HiveOptions.TABLE_EXEC_HIVE_SINK_STATISTIC_AUTO_GATHER_THREAD_NUM,
                                HiveOptions.STREAMING_SOURCE_ENABLE,
                                HiveOptions.STREAMING_SOURCE_PARTITION_INCLUDE,
                                HiveOptions.STREAMING_SOURCE_CONSUME_START_OFFSET,
                                HiveOptions.STREAMING_SOURCE_PARTITION_ORDER,
                                HiveOptions.LOOKUP_JOIN_CACHE_TTL,
                                HiveOptions.TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED,
                                HiveOptions.STREAMING_SOURCE_MONITOR_INTERVAL,
                                NoCatalogHiveOptions.CATALOG,
                                NoCatalogHiveOptions.PROPERTIES,
                                HiveCatalogFactoryOptions.HIVE_VERSION)
                        .collect(Collectors.toCollection(HashSet::new)));
        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper tableFactoryHelper =
                FactoryUtil.createTableFactoryHelper(this, context);
        tableFactoryHelper.validate();
        final Integer configuredSinkParallelism =
                Configuration.fromMap(context.getCatalogTable().getOptions())
                        .get(FileSystemConnectorOptions.SINK_PARALLELISM);

        HiveConf hiveConf = createHiveConf(tableFactoryHelper.getOptions());
        final JobConf jobConf = JobConfUtils.createJobConfWithCredentials(hiveConf);
        ObjectIdentifier objectIdentifier = createObjectIdentifier(tableFactoryHelper.getOptions());

        return new HiveTableSink(
                context.getConfiguration(),
                jobConf,
                objectIdentifier,
                context.getCatalogTable(),
                configuredSinkParallelism);
    }

    private ObjectIdentifier createObjectIdentifier(ReadableConfig options) {
        String tableName = options.get(NoCatalogHiveOptions.TABLE_NAME);
        String catalog = options.get(NoCatalogHiveOptions.CATALOG);
        String database = options.get(NoCatalogHiveOptions.DATABASE);
        return ObjectIdentifier.of(catalog, database, tableName);
    }

    private HiveConf createHiveConf(ReadableConfig options) {
        HiveConf hiveConf = new HiveConf();
        MetastoreConf.setVar(
                hiveConf,
                MetastoreConf.ConfVars.CATALOG_DEFAULT,
                options.get(NoCatalogHiveOptions.CATALOG));
        MetastoreConf.setVar(
                hiveConf,
                MetastoreConf.ConfVars.THRIFT_URIS,
                options.get(NoCatalogHiveOptions.METASTORE_THRIFT_URIS));
        hiveConf.set(
                HiveCatalogFactoryOptions.HIVE_VERSION.key(),
                options.get(HiveCatalogFactoryOptions.HIVE_VERSION));
        Map<String, String> properties = options.get(NoCatalogHiveOptions.PROPERTIES);
        properties.forEach(hiveConf::set);
        return hiveConf;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final ReadableConfig configuration =
                Configuration.fromMap(context.getCatalogTable().getOptions());

        HiveConf hiveConf = new HiveConf();

        final ResolvedCatalogTable catalogTable =
                Preconditions.checkNotNull(context.getCatalogTable());

        final boolean isStreamingSource = configuration.get(STREAMING_SOURCE_ENABLE);
        final boolean includeAllPartition =
                STREAMING_SOURCE_PARTITION_INCLUDE
                        .defaultValue()
                        .equals(configuration.get(STREAMING_SOURCE_PARTITION_INCLUDE));
        final JobConf jobConf = JobConfUtils.createJobConfWithCredentials(hiveConf);
        boolean readSubDirectory =
                context.getConfiguration()
                        .get(TABLE_EXEC_HIVE_READ_PARTITION_WITH_SUBDIRECTORY_ENABLED);
        // set whether to read directory recursively
        jobConf.set(FileInputFormat.INPUT_DIR_RECURSIVE, String.valueOf(readSubDirectory));

        // hive table source that has not lookup ability
        if (isStreamingSource && includeAllPartition) {
            return new HiveTableSource(
                    jobConf,
                    context.getConfiguration(),
                    context.getObjectIdentifier().toObjectPath(),
                    catalogTable);
        } else {
            // hive table source that has scan and lookup ability
            return new HiveLookupTableSource(
                    jobConf,
                    context.getConfiguration(),
                    context.getObjectIdentifier().toObjectPath(),
                    catalogTable);
        }
    }
}
