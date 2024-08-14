import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FilterSpaceString {
    private static final Logger LOG = LoggerFactory.getLogger(FilterSpaceString.class);

    public FilterSpaceString() {}

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("FilterSpaceString").getOrCreate();
        try (JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
            JavaRDD<String> parallelize =
                    jsc.parallelize(Arrays.stream(args).collect(Collectors.toList()));
            LOG.info("Current strings: {}", Arrays.toString(args));
            JavaRDD<String> filter = parallelize.filter((s) -> s.indexOf(' ') != -1);
            long count = filter.count();
            LOG.info("String has space count: {}", count);
            spark.logInfo(() -> "Result: " + count + " Strings: " + filter.collect());
        }
        spark.stop();
    }
}
