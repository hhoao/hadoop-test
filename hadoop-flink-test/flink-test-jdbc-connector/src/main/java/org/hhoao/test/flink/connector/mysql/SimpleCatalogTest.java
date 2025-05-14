package org.hhoao.test.flink.connector.mysql;

import java.util.List;
import java.util.Optional;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.hhoao.test.common.extension.MysqlContainerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * SimpleCatalogTest Catalog are used to manage metadata like databse、table not data.
 *
 * @author hhoa
 * @since 2023/7/17
 */
public class SimpleCatalogTest {
    @RegisterExtension
    MysqlContainerExtension mysqlContainerExtension = new MysqlContainerExtension();

    @Test
    void test() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        MysqlContainerExtension.MysqlProperties mysqlProperties =
                mysqlContainerExtension.getMysqlProperties();
        tableEnvironment.executeSql(
                String.format(
                        "CREATE CATALOG my_catalog WITH(\n"
                                + "    'type' = 'jdbc',\n"
                                + "    'default-database' = '%s', \n"
                                + "    'username' = '%s', \n"
                                + "    'password' = '%s', \n"
                                + "    'base-url' = '%s'\n"
                                + ");",
                        mysqlProperties.getDefaultDatabase(),
                        mysqlProperties.getUsername(),
                        mysqlProperties.getPassword(),
                        mysqlProperties.getUrl()));

        Optional<Catalog> myCatalog = tableEnvironment.getCatalog("my_catalog");
        Catalog catalog = myCatalog.get();
        List<String> strings = catalog.listDatabases();
        System.out.println(strings);
    }
}
