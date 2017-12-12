package org.workshop.broadcast;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.workshop.Airport;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by jayant on 10/28/17.
 */
public class BroadcastVariables {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkBasics").master("local")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        broadcast(spark, jsc);

        accululator(spark, jsc);

        spark.stop();
    }

    public static void broadcast(SparkSession spark, JavaSparkContext sc) {

        String[] arr = new String[] {"an", "Spark", "Installation"};
        HashMap<String, Integer> hashMap = new HashMap<>();
        for (String temp : arr) {
            hashMap.put(temp, 1);
        }

        Broadcast<HashMap<String, Integer>> broadcastVar = sc.broadcast(hashMap);

        JavaRDD<String> lines = spark.read().textFile("README.md").javaRDD();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {

                ArrayList<String> arrayList = new ArrayList<String>();

                String[] temp = s.split(" ");
                for (String str : temp) {
                    if (broadcastVar.value().containsKey(str) == false) {
                        arrayList.add(str);
                    }
                }

                return arrayList.iterator();
            }
        });

        int i = 0;
        List<String> list = words.collect();
        for (String str : list) {
            System.out.println(str);

            if (i > 100)
                break;
            i++;
        }

    }

    public static void accululator(SparkSession spark, JavaSparkContext sc) {

        LongAccumulator accum = sc.sc().longAccumulator();

        sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));

        Long value = accum.value();

        System.out.println("Accumulator Value : " + value);

        //-----

        LongAccumulator numCharactersAccum = sc.sc().longAccumulator();

        JavaRDD<String> lines = spark.read().textFile("README.md").javaRDD();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                numCharactersAccum.add(s.length());
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        words.count();

        System.out.println("Total number of characters Value : " + numCharactersAccum.value());

    }
}
