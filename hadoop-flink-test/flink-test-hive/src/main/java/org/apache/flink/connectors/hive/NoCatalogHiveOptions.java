package org.apache.flink.connectors.hive;

import static org.apache.flink.configuration.ConfigOptions.key;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

/**
 * NewHiveOptions
 *
 * @author w
 * @since 2024/9/10
 */
public class NoCatalogHiveOptions {
    public static final ConfigOption<String> METASTORE_THRIFT_URIS =
            key("metastore.thrift.uris").stringType().noDefaultValue().withDescription("");
    public static final ConfigOption<String> CATALOG =
            key("catalog")
                    .stringType()
                    .defaultValue(MetastoreConf.ConfVars.CATALOG_DEFAULT.getDefaultVal().toString())
                    .withDescription("");
    public static final ConfigOption<String> TABLE_NAME =
            key("table-name").stringType().noDefaultValue().withDescription("");
    public static final ConfigOption<String> DATABASE =
            key("database").stringType().defaultValue("default").withDescription("");
    public static final ConfigOption<Map<String, String>> PROPERTIES =
            key("properties").mapType().defaultValue(new HashMap<>()).withDescription("");
    public static final ConfigOption<String> SECURITY_KERBEROS_KRB5_CONF =
            key("security.kerberos.krb5.conf").stringType().noDefaultValue().withDescription("");
    public static final ConfigOption<String> SECURITY_KERBEROS_PRINCIPAL =
            key("security.kerberos.principal").stringType().noDefaultValue().withDescription("");
    public static final ConfigOption<String> SECURITY_KERBEROS_KEYTAB =
            key("security.kerberos.keytab").stringType().noDefaultValue().withDescription("");
}
