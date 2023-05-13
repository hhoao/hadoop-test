// package org.example;
//
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;
//
// import java.io.IOException;
// import java.net.URL;
// import java.sql.*;
//
// public class HiveJdbcClient {
//    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
//
//    /**
//     * @param args
//     * @throws SQLException
//     */
//    public static void main(String[] args) throws SQLException, IOException {
//        try {
//            Class.forName(DRIVER_NAME);
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//        String user = System.getenv("USER");
//        //replace "hive" here with the name of the user the queries should run as
//        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", user,
// "");
//        Statement stmt = con.createStatement();
//        String tableName = "testHiveDriverTable";
//        stmt.execute("drop table if exists " + tableName);
//        stmt.execute("CREATE TABLE " + tableName + " (userid INT,movieid INT,rating INT,unixtime
// STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE");
//
//        // show tables
//        String sql = "show tables '" + tableName + "'";
//        System.out.println("Running: " + sql);
//        ResultSet res = stmt.executeQuery(sql);
//        if (res.next()) {
//            System.out.println(res.getString(1));
//        }
//        // describe table
//        sql = "describe " + tableName;
//        System.out.println("Running: " + sql);
//        res = stmt.executeQuery(sql);
//        while (res.next()) {
//            System.out.println(res.getString(1) + "\t" + res.getString(2));
//        }
//
//        // load data into table
//        // NOTE: filepath has to be local to the hive server
//        String hadoopHome = System.getenv("HADOOP_HOME");
//        Configuration configuration = new Configuration();
//        configuration.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
//        configuration.addResource(new Path(hadoopHome + "/etc/hadoop/hdfs-site.xml"));
//        configuration.addResource(new Path(hadoopHome + "/etc/hadoop/yarn-site.xml"));
//
//        FileSystem fileSystem = FileSystem.get(configuration);
//        URL resource = ClassLoader.getSystemClassLoader().getResource("u.data");
//        fileSystem.copyFromLocalFile(new Path(resource.getFile()), new Path("/tmp/u.data"));
//
//        String filepath = "/tmp/u.data";
//        sql = "LOAD DATA LOCAL INPATH '" + filepath + "' OVERWRITE INTO TABLE " + tableName;
//        System.out.println("Running: " + sql);
//        stmt.execute(sql);
//
//        // select * query
//        sql = "select * from " + tableName;
//        System.out.println("Running: " + sql);
//        res = stmt.executeQuery(sql);
//        while (res.next()) {
//            System.out.println(res.getInt(1) + "\t" + res.getString(2));
//        }
//
//        // regular hive query
//        sql = "select count(1) from " + tableName;
//        System.out.println("Running: " + sql);
//        res = stmt.executeQuery(sql);
//        while (res.next()) {
//            System.out.println(res.getString(1));
//        }
//    }
// }
