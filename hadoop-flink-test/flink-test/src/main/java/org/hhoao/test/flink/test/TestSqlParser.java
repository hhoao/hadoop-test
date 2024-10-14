package org.hhoao.test.flink.test;

import java.util.HashMap;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;

/**
 * TestSqlParser
 *
 * @author w
 * @since 2024/10/1
 */
public class TestSqlParser {
    public static void main(String[] args) throws SqlParseException {
        TableConfig tableConfig = TableConfig.getDefault();
        CalciteConfig calciteConfig = TableConfigUtils.getCalciteConfig(tableConfig);
        SqlParser.Config config =
                JavaScalaConversionUtil.toJava(calciteConfig.getSqlParserConfig())
                        .orElseGet(
                                () -> {
                                    SqlConformance conformance = FlinkSqlConformance.DEFAULT;
                                    return SqlParser.config()
                                            .withParserFactory(
                                                    FlinkSqlParserFactories.create(conformance))
                                            .withConformance(conformance)
                                            .withLex(Lex.JAVA)
                                            .withIdentifierMaxLength(256);
                                });
        SqlParser parser =
                SqlParser.create(
                        "CREATE TABLE sink (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  age INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'print'\n"
                                + ");",
                        config);
        SqlNodeList sqlNodes = parser.parseStmtList();
        System.out.println(sqlNodes.getList());
        HashMap<String, String> options = getOptions(sqlNodes);
        Factory factory =
                FactoryUtil.discoverFactory(
                        TestSqlParser.class.getClassLoader(),
                        Factory.class,
                        options.get(FactoryUtil.CONNECTOR.key()));
        if (factory instanceof DynamicTableFactory) {
            FactoryUtil.FactoryHelper<DynamicTableFactory> factoryHelper =
                    new FactoryUtil.FactoryHelper<>(
                            (DynamicTableFactory) factory, options, FactoryUtil.CONNECTOR);
            factoryHelper.validate();
        }
    }

    private static HashMap<String, String> getOptions(SqlNodeList sqlNodes) {
        HashMap<String, String> options = new HashMap<>();
        for (SqlNode sqlNode : sqlNodes) {
            if (sqlNode instanceof SqlCreateTable) {
                SqlNodeList propertyList = ((SqlCreateTable) sqlNode).getPropertyList();
                propertyList.forEach(
                        node -> {
                            options.put(
                                    ((SqlTableOption) node).getKeyString(),
                                    ((SqlTableOption) node).getValueString());
                        });
            }
        }
        return options;
    }
}
