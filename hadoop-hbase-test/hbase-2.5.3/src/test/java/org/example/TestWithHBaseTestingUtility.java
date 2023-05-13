package org.example;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;

/**
 * TestForHBaseTestingUtility
 *
 * @author hhoa
 * @since 2023/5/11
 */
public class TestWithHBaseTestingUtility {
    @Test
    public void test() throws Exception {
        HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();
        hBaseTestingUtility.startMiniZKCluster();
        hBaseTestingUtility.startMiniHBaseCluster();
        Table table = hBaseTestingUtility.createTable(TableName.valueOf("TestTable"), "TestFamily");
        Put put = new Put("TestRow".getBytes());
        put.addColumn("TestFamily".getBytes(), "TestQualifier".getBytes(), "Value".getBytes());
        table.put(put);
        Get get = new Get("TestRow".getBytes());
        Result result = table.get(get);
        byte[] value = result.getValue("TestFamily".getBytes(), "TestQualifier".getBytes());
        Assert.assertEquals(new String(value), "Value");
        hBaseTestingUtility.closeConnection();
        hBaseTestingUtility.shutdownMiniHBaseCluster();
    }
}
