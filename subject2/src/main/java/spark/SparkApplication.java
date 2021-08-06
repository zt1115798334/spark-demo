package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 *
 * @author zhang
 * date: 2021/8/5 16:32
 * description:
 */
public class SparkApplication {
    private static final Pattern SPACE = Pattern.compile("");
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("sparkBoot").setMaster("127.0.0.1:8080");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sparkContext.textFile("D:\\IdeaProjects\\spark-demo\\docker-compose\\data").cache();
        lines.map((Function<String, Object>) s -> s);
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> wordsOnes = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> wordCounts = wordsOnes.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        wordCounts.saveAsTextFile("D:\\IdeaProjects\\spark-demo\\docker-compose\\output");


    }
}
