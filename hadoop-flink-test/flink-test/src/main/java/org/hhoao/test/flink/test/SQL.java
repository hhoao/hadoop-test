package org.hhoao.test.flink.test;

import java.time.Duration;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.*;

public class SQL {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        Configuration configuration = settings.getConfiguration();

        configuration.set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        configuration.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 6);
        configuration.set(StateBackendOptions.STATE_BACKEND, "jobmanager");

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.createTemporaryTable(
                "SourceTable",
                TableDescriptor.forConnector("datagen")
                        .schema(Schema.newBuilder().column("f0", DataTypes.STRING()).build())
                        .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                        .build());
        tableEnv.createTemporaryTable(
                "SourceTable1",
                TableDescriptor.forConnector("datagen")
                        .schema(Schema.newBuilder().column("f0", DataTypes.STRING()).build())
                        .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                        .build());

        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING OPTIONS) ");
        tableEnv.executeSql(
                "INSERT INTO SinkTable SELECT s0.* FROM SourceTable s0 left join SourceTable s1 ON 1=1");
    }
}
