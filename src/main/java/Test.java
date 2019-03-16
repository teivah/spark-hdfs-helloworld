import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Test {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App")
        .set("spark.hadoop.dfs.client.use.datanode.hostname", "true");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> input = sc.textFile("hdfs://hadoop:9000/audit.log");
    JavaRDD<String> words = input.flatMap(
        (FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")));
    JavaPairRDD<String, Integer> counts = words.mapToPair(
        (PairFunction<String, String, Integer>) s -> new Tuple2(s, 1));
    JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(
        (Function2<Integer, Integer, Integer>) (x, y) -> x + y);
    reducedCounts.saveAsTextFile("output");
  }
}
