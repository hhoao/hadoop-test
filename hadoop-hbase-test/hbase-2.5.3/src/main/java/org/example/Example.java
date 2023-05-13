package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

public class Example {
    private static final String TABLE_NAME = "testTable";
    private static final String CF_DEFAULT = "testFamily";
    private static final String hbaseConfDir =
            "/home/hhoa/applications/hbase-2.4.17-bin/hbase-2.4.17/conf";
    private static final String hadoopConfDir =
            "/home/hhoa/.sdkman/candidates/hadoop/current/conf/hadoop";

    private static void startCRUD(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()) {
            // Create or override has CF_DEFAULT family table
            TableDescriptorBuilder.ModifyableTableDescriptor table =
                    new TableDescriptorBuilder.ModifyableTableDescriptor(
                            TableName.valueOf(TABLE_NAME));
            table.setColumnFamily(
                    new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(
                                    CF_DEFAULT.getBytes())
                            .setCompressionType(Algorithm.NONE));
            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);

            //  Is table exist
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            // Update existing table
            ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor newColumn =
                    new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(
                            "NEWCF".getBytes());
            newColumn.setCompactionCompressionType(Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumnFamily(tableName, newColumn);

            // Update existing column family
            ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor existingColumn =
                    new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(
                            CF_DEFAULT.getBytes());
            existingColumn.setCompactionCompressionType(Algorithm.GZ);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.modifyColumnFamily(tableName, existingColumn);

            // Disable an existing table
            admin.disableTable(tableName);

            // Delete an existing column family
            admin.deleteColumnFamily(tableName, CF_DEFAULT.getBytes());

            // Delete a table (Need to be disabled first)
            admin.deleteTable(tableName);
        }
    }

    public static void main(String... args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        // Add any necessary configuration files (hbase-site.xml, core-site.xml)
        // config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        // config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        config.addResource(new Path(hbaseConfDir, "hbase-site.xml"));
        config.addResource(new Path(hadoopConfDir, "core-site.xml"));
        startCRUD(config);
    }
}
