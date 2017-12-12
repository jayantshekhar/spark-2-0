package org.workshop.streaming;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.workshop.Flight;

public class FlightsStreaming implements Serializable{

    public static void main(String[] args) throws InterruptedException, IOException {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARNING);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("JavaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(5000));

        JavaDStream<String> flightsData = jssc.textFileStream("data/flights").cache();

        JavaDStream<String> windowFlightsData = flightsData.window(Durations.seconds(50));

        flightsData.print();

        flightsData.foreachRDD((rdd, time) -> {
            // Get the singleton instance of SparkSession
            SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();

            // Convert RDD[String] to RDD[case class] to DataFrame
            JavaRDD<Flight> rowRDD = rdd.map(line -> {
                Flight flight = new Flight(line.split(","));
                return flight;
            });
            Dataset<Row> flights = spark.createDataFrame(rowRDD, Flight.class);

            // Creates a temporary view using the DataFrame
            flights.createOrReplaceTempView("flights");

            // Do word count on table using SQL and print it
            Dataset<Row> tailNumCounts =
                    spark.sql("select TAIL_NUM, count(*) as total from flights group by TAIL_NUM");

            tailNumCounts.printSchema();
            tailNumCounts.show();

            Dataset<Row> numFlightsFromOrigin =
                    spark.sql("select ORIGIN, count(*) as total from flights group by ORIGIN");

            numFlightsFromOrigin.printSchema();
            numFlightsFromOrigin.show();
        });

        jssc.start();
        jssc.awaitTermination();

    }


}


