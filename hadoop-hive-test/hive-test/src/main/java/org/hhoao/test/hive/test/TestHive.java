package org.hhoao.test.hive.test;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.hhoao.test.hive.base.HiveTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HiveServerSimpleTest
 *
 * @author hhoa
 * @since 2023/5/9
 */
public class TestHive extends HiveTest {
    private static final Logger LOG = LoggerFactory.getLogger(TestHive.class);

    @Test
    public void testCrud() throws SQLException {
        LOG.info("Create tables");
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        String tableName = "test_hive";
        String dropTablePrepareStatement = "DROP TABLE IF EXISTS %s";
        statement.execute(String.format(dropTablePrepareStatement, tableName));
        String createTablePrepareStatement =
                "CREATE TABLE %s (userid INT,movieid INT,rating INT, unixtime STRING)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE ";
        statement.execute(String.format(createTablePrepareStatement, tableName));

        LOG.info("Show tables");
        String showTablesPrepareStatement = "show tables '%s'";
        ResultSet res =
                statement.executeQuery(String.format(showTablesPrepareStatement, tableName));
        if (res.next()) {
            Assertions.assertEquals(tableName.toLowerCase(), res.getString(1));
        }

        LOG.info("Describe table");
        String describeTablePrepareStatement = "DESCRIBE %s";
        res = statement.executeQuery(String.format(describeTablePrepareStatement, tableName));
        while (res.next()) {
            String colName = res.getString(1);
            String type = res.getString(2);
            switch (colName) {
                case "userid":
                case "rating":
                case "movieid":
                    Assertions.assertEquals("int", type);
                    break;
                case "unixtime":
                    Assertions.assertEquals("string", type);
            }
        }

        LOG.info("Load data into table");
        File dataFile =
                new File(ClassLoader.getSystemClassLoader().getResource("u.data").getPath());

        String loadDataPrepareStatement = "LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s";
        statement.execute(
                String.format(loadDataPrepareStatement, dataFile.getAbsolutePath(), tableName));

        String selectAllPrepareStatement = "SELECT * FROM %s WHERE userid=303 AND movieid=55";
        res = statement.executeQuery(String.format(selectAllPrepareStatement, tableName));
        while (res.next()) {
            Assertions.assertEquals(303, res.getInt("userid"));
            Assertions.assertEquals(55, res.getInt("movieid"));
        }

        LOG.info("Regular hive query");
        String countPrepareStatement = "SELECT COUNT(*) FROM %s";
        res = statement.executeQuery(String.format(countPrepareStatement, tableName));
        res.next();
        Assertions.assertEquals(100000, res.getInt(1));
    }
}
