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
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class FlightsStreaming implements Serializable{

    public static void main(String[] args) throws InterruptedException, IOException {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARNING);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("JavaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(5000));

        JavaDStream<String> flightsData = jssc.textFileStream("data/flights").cache();

        flightsData.print();

        flightsData.foreachRDD((rdd, time) -> {
            // Get the singleton instance of SparkSession
            SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();

            // Convert RDD[String] to RDD[case class] to DataFrame
            JavaRDD<JavaRow> rowRDD = rdd.map(word -> {
                JavaRow record = new JavaRow();
                record.setWord(word);
                return record;
            });
            Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRow.class);

            // Creates a temporary view using the DataFrame
            wordsDataFrame.createOrReplaceTempView("words");

            // Do word count on table using SQL and print it
            Dataset<Row> wordCountsDataFrame =
                    spark.sql("select word, count(*) as total from words group by word");
            wordCountsDataFrame.show();
        });

        jssc.start();
        jssc.awaitTermination();

    }


}


