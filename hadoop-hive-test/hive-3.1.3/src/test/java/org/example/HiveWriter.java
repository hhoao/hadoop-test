package org.example;

import com.sun.tools.javac.util.List;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

/**
 * HiveWriter
 *
 * @author xianxing
 * @since 2024/7/11
 */
public class HiveWriter {
    @Test
    public void test2()
            throws ClassNotFoundException, IllegalAccessException, InstantiationException,
                    HiveException, IOException, SerDeException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "172.16.21.253:9000");

        StorageDescriptor sd = Table.getEmptyTable(null, null).getSd();
        SerDeInfo serDeInfo = new SerDeInfo();
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(serdeConstants.SERIALIZATION_FORMAT, "1");
        serDeInfo.setParameters(parameters);
        serDeInfo.setSerializationLib(MetadataTypedColumnsetSerDe.class.getName());
        sd.setInputFormat(SequenceFileInputFormat.class.getName());
        sd.setOutputFormat(HiveSequenceFileOutputFormat.class.getName());

        StorageFormatFactory storageFormatFactory = new StorageFormatFactory();
        sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
        // 通过格式类型获取StorageFormatDescriptor，这里一般有TEXT、AVRO、PARQUET、ORC这几种，可通过IOConstants查看
        StorageFormatDescriptor storageFormatDescriptor =
                storageFormatFactory.get(IOConstants.PARQUET);
        sd.setInputFormat(storageFormatDescriptor.getInputFormat());
        sd.setOutputFormat(storageFormatDescriptor.getOutputFormat());
        String serdeLib = storageFormatDescriptor.getSerde();
        if (serdeLib != null) {
            sd.getSerdeInfo().setSerializationLib(serdeLib);
        }
        SerDeInfo serdeInfo = sd.getSerdeInfo();
        Properties tableProperties = new Properties();
        //        tableProperties.put(serdeConstants.LIST_COLUMNS, "");
        //        tableProperties.put(serdeConstants.LIST_COLUMN_TYPES, "");
        //        tableProperties.put(serdeConstants.FIELD_DELIM, (byte) 1);
        tableProperties.setProperty(serdeConstants.FIELD_DELIM, ",");
        //        tableProperties.setProperty(serdeConstants.COLLECTION_DELIM, "");
        //        tableProperties.setProperty(serdeConstants.MAPKEY_DELIM, "");
        Serializer recordSerDe =
                (Serializer) (Class.forName(serdeInfo.getSerializationLib()).newInstance());
        SerDeUtils.initializeSerDe(
                (Deserializer) recordSerDe, configuration, tableProperties, null);
        Class<? extends OutputFormat> outputFormatClz =
                HiveFileFormatUtils.getOutputFormatSubstitute(
                        Class.forName(storageFormatDescriptor.getOutputFormat()));
        HiveOutputFormat outputFormat = (HiveOutputFormat) outputFormatClz.newInstance();
        // 这里对应hive相应的表、分区路径、还有一个随机的文件名
        Path path =
                new Path("/dtInsight/hive/warehouse/array_type_parquet/pt_day=12/pt_hour=12/test");
        JobConf jobConf = new JobConf(configuration);
        jobConf.setMapOutputCompressorClass(GzipCodec.class);
        jobConf.set(
                org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC,
                GzipCodec.class.getName());
        FileSinkOperator.RecordWriter recordWriter =
                HiveFileFormatUtils.getRecordWriter(
                        jobConf,
                        outputFormat,
                        recordSerDe.getSerializedClass(),
                        false,
                        tableProperties,
                        path,
                        Reporter.NULL);

        ObjectInspector intInspector =
                ObjectInspectorFactory.getReflectionObjectInspector(
                        Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        StandardListObjectInspector intListInspector =
                ObjectInspectorFactory.getStandardListObjectInspector(intInspector);
        StandardStructObjectInspector standardStructObjectInspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(
                        new ArrayList<>(List.of("address")),
                        new ArrayList<>(Arrays.asList(intListInspector)));

        Object[] instance = new Object[1];
        ArrayList<Integer> address = new ArrayList<>();
        for (int i = 5; i < 10; i++) {
            address.add(i * i);
        }
        instance[0] = address;
        Writable serialize = recordSerDe.serialize(instance, standardStructObjectInspector);
        recordWriter.write(serialize);
        recordWriter.close(false);
    }

    @Test
    public void testWriteIdWithArrayArray2() {
        UserGroupInformation admin = UserGroupInformation.createRemoteUser("admin");
        admin.doAs(
                (PrivilegedAction<Void>)
                        () -> {
                            try {
                                Configuration configuration = new Configuration();
                                //                            configuration.set("fs.defaultFS",
                                // "172.16.21.253:9000");
                                configuration.set("fs.defaultFS", "172.16.20.255:9000");
                                Path path =
                                        new Path(
                                                "/dtInsight/hive/warehouse/array_type_parquet/pt_day=12/pt_hour=12/test");
                                Types.MessageTypeBuilder messageTypeBuilder = Types.buildMessage();
                                PrimitiveType id =
                                        Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                                .named("id");
                                PrimitiveType named =
                                        Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                                .named("address");

                                messageTypeBuilder.addField(id);
                                messageTypeBuilder.addField(id);
                                messageTypeBuilder
                                        .optionalList()
                                        .optionalListElement()
                                        .element(named)
                                        .named("address")
                                        .named("address");
                                MessageType pari = messageTypeBuilder.named("Pair");
                                SimpleGroup simpleGroup = new SimpleGroup(pari);
                                simpleGroup.add(0, 100);
                                simpleGroup.add(1, 200);
                                // add group
                                Group address = simpleGroup.addGroup(2);
                                for (int i = 0; i < 5; i++) {
                                    // group add list entry
                                    Group listGroup = address.addGroup(0);
                                    // add group
                                    Group sublist = listGroup.addGroup(0);
                                    for (int j = 5; j < 10; j++) {
                                        // group add list entry
                                        Group subListGroup = sublist.addGroup(0);
                                        subListGroup.add(0, i * i);
                                    }
                                }
                                ExampleParquetWriter.Builder parquetWriterBuilder =
                                        getParquetWriterBuilder(path, configuration, pari);
                                FileSystem fileSystem = FileSystem.get(configuration);
                                new Thread(
                                                () -> {
                                                    try {
                                                        for (int j = 0; j < 1000; j++) {
                                                            ParquetWriter<Group> parquetWriter =
                                                                    parquetWriterBuilder.build();
                                                            for (int i = 0; i < 1000; i++) {
                                                                parquetWriter.write(simpleGroup);
                                                            }
                                                            System.out.println(j);
                                                            parquetWriter.close();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                    System.out.println("down1");
                                                })
                                        .start();
                                //                                new Thread(() -> {
                                //                                    try {
                                //                                        for (int i = 0; i <
                                // 100000; i++) {
                                //
                                // parquetWriter1.write(simpleGroup);
                                //                                        }
                                //                                        parquetWriter1.close();
                                //                                    } catch (Exception e) {
                                //                                        throw new
                                // RuntimeException(e);
                                //                                    }
                                //                                    System.out.println("down2");
                                //                                }).start();
                                Thread.sleep(1000000);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        });
    }

    private static ExampleParquetWriter.Builder getParquetWriterBuilder(
            Path path, Configuration configuration, MessageType pari) throws IOException {
        return ExampleParquetWriter.builder(path)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withConf(configuration)
                .withType(pari)
                .withDictionaryEncoding(false)
                .withRowGroupSize(1000000000);
    }
}
