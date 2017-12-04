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
package org.strata.workshop

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


import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Housing {

  case class X(
                id: String ,price: Double, lotsize: Double, bedrooms: Double,
                bathrms: Double,stories: Double, driveway: String,recroom: String,
                fullbase: String, gashw: String, airco: String, garagepl: Double, prefarea: String)

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    var input = "data/Housing.csv"
    if (args.length > 0) {
      input = args(0)
    }

    val spark = SparkSession
      .builder
      .appName("LinearRegressionWithEncoding")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val data = spark.sparkContext.textFile(input)
      .map(_.split(","))
      .map( x => ( X(
        x(0), x(1).toDouble, x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble,
        x(6), x(7), x(8), x(9), x(10), x(11).toDouble, x(12) )))
      .toDF()

    data.show()

    data.createOrReplaceTempView("house")

    val sqlDF = spark.sql("SELECT * FROM house where stories > 2")
    sqlDF.show()

  }
}
