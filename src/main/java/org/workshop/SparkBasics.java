package org.workshop;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jayant on 10/27/17.
 */
public final class SparkBasics {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkBasics").master("local")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        parallelizedCollections(jsc);

        readFile(jsc);

        readTransformAct(jsc);

        passingFunctions(jsc);

        passingFunctions_1(jsc);

        spark.stop();
    }

    public static void parallelizedCollections(JavaSparkContext jsc) {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = jsc.parallelize(data);

        distData.collect().forEach(i -> System.out.println(i));
    }

    public static void readFile(JavaSparkContext jsc) {
        JavaRDD<String> distFile = jsc.textFile("README.md", 1);

        distFile.collect().forEach(i -> System.out.println(i));
    }

    public static void readTransformAct(JavaSparkContext jsc) {
        JavaRDD<String> lines = jsc.textFile("data/Housing.csv", 1);
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());

        lineLengths.collect().forEach(i -> System.out.println(i));

        int totalLength = lineLengths.reduce((a, b) -> a + b);
    }


    public static void passingFunctions(JavaSparkContext jsc) {
        JavaRDD<String> lines = jsc.textFile("data/Housing.csv", 1);

        JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
            public Integer call(String s) { return s.length(); }
        });

        int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });

    }

    public static void passingFunctions_1(JavaSparkContext jsc) {
        JavaRDD<String> lines = jsc.textFile("README.md");
        JavaRDD<Integer> lineLengths = lines.map(new GetLength());
        int totalLength = lineLengths.reduce(new Sum());

        System.out.println("Total Length : "+totalLength);
    }
}


class GetLength implements Function<String, Integer> {
    public Integer call(String s) { return s.length(); }
}
class Sum implements Function2<Integer, Integer, Integer> {
    public Integer call(Integer a, Integer b) { return a + b; }
}
