package org.workshop

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.math._

object Basics {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark Pi").master("local")
      .getOrCreate()

    parallelizedCollections(spark.sparkContext)

    mapReduce(spark.sparkContext)

    keyValuePairs(spark.sparkContext)

    spark.stop()
  }

  def parallelizedCollections(sc : SparkContext): Unit = {
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    distData.take(100).foreach(println)
  }

  def mapReduce(sc : SparkContext): Unit = {

    val lines = sc.textFile("data/Housing.csv")
    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)

    lineLengths.take(100).foreach(println)

  }

  def keyValuePairs(sc : SparkContext): Unit = {
    val lines = sc.textFile("data/Housing.csv")
    val pairs = lines.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)

    counts.take(100).foreach(println)
  }

}
