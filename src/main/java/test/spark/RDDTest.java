package test.spark;

import com.google.common.base.Splitter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("RDDTest").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> data = Arrays.asList(1, 23, 4, 5, 6);
		JavaRDD<Integer> distData = sc.parallelize(data);
		System.out.println("distData = " + distData.count());

		JavaRDD<String> fileRDD = sc.textFile("src/main/resources/people.txt");
		Integer count = fileRDD.map(new Function<String, Integer>() {
			public Integer call(String s) throws Exception {
				return s.length();
			}
		}).reduce(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer int1, Integer int2) throws Exception {
				return int1 + int2;
			}
		});
		System.out.println(count);

		JavaRDD<String> lines = sc.textFile("src/main/resources/data.txt");

		JavaPairRDD<String, Integer> pairs = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) throws Exception {
				return Splitter.onPattern("[ ,.:]").split(s);
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer int1, Integer int2) throws Exception {
				return int1 + int2;
			}
		});

		counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			public void call(Tuple2<String, Integer> tuple2) throws Exception {
				System.out.println(tuple2._1() + ": " + tuple2._2());
			}
		});

		counts.saveAsTextFile("src/main/resources/data-trans");
	}
}