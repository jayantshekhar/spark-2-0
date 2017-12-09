package org.workshop.broadcast;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

/**
 * Created by jayant on 10/28/17.
 */
public class BroadcastVariables {

    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkBasics").master("local")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        broadcast(jsc);

        accululator(jsc);

        spark.stop();
    }

    public static void broadcast(JavaSparkContext sc) {

        Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});

        int[] temp = broadcastVar.value();

    }

    public static void accululator(JavaSparkContext sc) {

        LongAccumulator accum = sc.sc().longAccumulator();

        sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));

        Long value = accum.value();

        System.out.println("Accumulator Value : " + value);
    }
}
