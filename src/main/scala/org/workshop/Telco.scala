/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.strata.workshop

// $example on$

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types._

// $example off$
import org.apache.spark.sql.SparkSession


object Telco {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName("ChurnExample")
      .master("local")
      .getOrCreate()

    // define the schema of input data
    val customSchema = StructType(Array(
      StructField("state", StringType, true),
      StructField("account_length", DoubleType, true),
      StructField("area_code", StringType, true),

      StructField("phone_number", StringType, true),
      StructField("intl_plan", StringType, true),

      StructField("voice_mail_plan", StringType, true),
      StructField("number_vmail_messages", DoubleType, true),

      StructField("total_day_minutes", DoubleType, true),
      StructField("total_day_calls", DoubleType, true),

      StructField("total_day_charge", DoubleType, true),
      StructField("total_eve_minutes", DoubleType, true),
      StructField("total_eve_calls", DoubleType, true),
      StructField("total_eve_charge", DoubleType, true),
      StructField("total_night_minutes", DoubleType, true),

      StructField("total_night_calls", DoubleType, true),
      StructField("total_night_charge", DoubleType, true),
      StructField("total_intl_minutes", DoubleType, true),
      StructField("total_intl_calls", DoubleType, true),
      StructField("total_intl_charge", DoubleType, true),
      StructField("number_customer_service_calls", DoubleType, true),

      StructField("churned", StringType, true)

    ))

    // read in the data into a DataFrame
    val ds = spark.read.option("inferSchema", "true").schema(customSchema).csv("data/churn.all")
    ds.printSchema()

    ds.createOrReplaceTempView("telco")

    val sqlDF = spark.sql("SELECT * FROM telco where number_vmail_messages > 2")
    sqlDF.show()

    // read in the data into a DataFrame
    val state = spark.read.option("inferSchema", "true").option("header", "true").csv("data/state.csv")
    state.printSchema()
    state.show()

    ds.createOrReplaceTempView("stateds")

    //val joined = spark.sql("SELECT * FROM telco, stateds where telco.state = stateds.abbreviation")
    val joined = ds.join(state, "state")
    joined.show()

    spark.stop()
  }
}
// scalastyle:on println


