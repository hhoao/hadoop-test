package org.hhoao.test.flink.hive.test;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.beeline.BeeLine;
import org.hhoao.hadoop.test.utils.Resources;
import org.hhoao.test.flink.hive.source.User;
import org.hhoao.test.flink.hive.source.UserIteratorSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TestHiveConnector
 *
 * @author w
 * @since 2024/9/9
 */
public class TestNoKerberosCacheHiveConnector {
    private static final String TABLE_NAME = "user_test";
    private HiveConf hiveConf;
    private static final File KRB_CONF =
            new File(
                    "/Users/w/sync/development/large-scale-data/hadoop/study/hadoop-test/hadoop-hive-test/hive-test/target/hadoop/1726486101754/kdc/1726486101763/krb5.conf");
    private static final File HIVE_CONF =
            new File(
                    "/Users/w/sync/development/large-scale-data/hadoop/study/hadoop-test/hadoop-hive-test/hive-test/target/hive/1726486101740/hive-site.xml");

    @BeforeEach
    void beforeTest() throws IOException, SQLException {
        System.setProperty("java.security.krb5.conf", KRB_CONF.getAbsolutePath());
        hiveConf = new HiveConf();
        hiveConf.addResource(new Path(HIVE_CONF.getAbsolutePath()));
        String principal =
                MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.KERBEROS_PRINCIPAL);
        String keytab = MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE);
        String user = System.getenv("USER");
        int port = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT);
        String url = String.format("jdbc:hive2://localhost:%s/default", port);
        Connection connection =
                DriverManager.getConnection(
                        String.format("jdbc:hive2://localhost:%s/default", port), user, "");
        UserGroupInformation ugi =
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        ugi.doAs(
                (PrivilegedAction<?>)
                        () -> {
                            try {
                                Statement statement = connection.createStatement();
                                String createTablePrepareStatement =
                                        "CREATE TABLE IF NOT EXISTS user_test(\n"
                                                + "    id         INT,\n"
                                                + "    name       VARCHAR(127),\n"
                                                + "    money      DECIMAL,\n"
                                                + "    age        INT\n"
                                                + ") partitioned by (pt string)";
                                statement.execute(createTablePrepareStatement);

                                CompletableFuture.runAsync(
                                        () -> {
                                            try (BeeLine beeLine = new BeeLine()) {
                                                beeLine.begin(
                                                        new String[] {"-u", url, "-n", user},
                                                        System.in);
                                                TimeUnit.HOURS.sleep(1);
                                            } catch (InterruptedException | IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        });
                                return null;
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
        UserGroupInformation.reset();
    }

    @Test
    public void test() {
        Configuration configuration = new Configuration();

        configuration.set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30));
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                new File(Resources.getTargetDir(), "checkpoints").toURI().toString());

        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment streamTableEnvironment =
                StreamTableEnvironment.create(executionEnvironment);
        DataStreamSource<User> userDataStreamSource =
                executionEnvironment.addSource(new UserIteratorSource());

        String uri = MetastoreConf.getAsString(hiveConf, MetastoreConf.ConfVars.THRIFT_URIS);
        String principal =
                MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.KERBEROS_PRINCIPAL);
        String keytab = MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE);

        streamTableEnvironment.executeSql(
                String.format(
                        "CREATE TABLE user_test (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  money DECIMAL,\n"
                                + "  age INT,\n"
                                + "  pt STRING,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") PARTITIONED BY (pt) WITH (\n"
                                + "   'connector' = 'hive',\n"
                                + "   'table-name' = '%s',\n"
                                + "   'database' = 'default',\n"
                                + "   'metastore.thrift.uris' = '%s',\n"
                                + "   'sink.partition-commit.policy.kind' = 'metastore', \n"
                                + "   'security.kerberos.keytab'='%s', \n"
                                + "   'security.kerberos.principal'='%s', \n"
                                + "   'properties.hive.in.test' = 'true'\n"
                                + ");",
                        TABLE_NAME, uri, keytab, principal));
        Table table = streamTableEnvironment.fromDataStream(userDataStreamSource);

        streamTableEnvironment.createTemporaryView("T", table);
        TableResult tableResult =
                streamTableEnvironment.executeSql(
                        "INSERT INTO user_test " + "SELECT id, name, money, age, 'hello' FROM T");
        tableResult.print();
    }
}
