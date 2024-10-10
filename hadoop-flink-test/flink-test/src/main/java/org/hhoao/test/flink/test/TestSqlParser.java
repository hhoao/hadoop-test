package org.hhoao.test.flink.test;

import java.util.List;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

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
                        "CREATE TABLE source (\n"
                                + "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',\n"
                                + "  `partition` BIGINT METADATA VIRTUAL,\n"
                                + "  `offset` BIGINT METADATA VIRTUAL,\n"
                                + "  `id` INT,\n"
                                + "  `name` STRING,\n"
                                + "  `age` INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'value.format' = 'json'\n"
                                + ");",
                        config);
        SqlNodeList sqlNodes = parser.parseStmtList();
        List<@Nullable SqlNode> list = sqlNodes.getList();
        System.out.println(list);
    }
}
