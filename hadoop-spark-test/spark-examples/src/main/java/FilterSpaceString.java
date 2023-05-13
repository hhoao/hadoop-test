import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public final class FilterSpaceString {
    public FilterSpaceString() {}

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("FilterSpaceString").getOrCreate();
        try (JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
            JavaRDD<String> parallelize =
                    jsc.parallelize(Arrays.stream(args).collect(Collectors.toList()));
            System.out.println("Current strings:" + Arrays.toString(args));
            JavaRDD<String> filter = parallelize.filter((s) -> s.indexOf(' ') != -1);
            long count = filter.count();
            System.out.println("String has space count: " + count);
        }
        spark.stop();
    }
}
