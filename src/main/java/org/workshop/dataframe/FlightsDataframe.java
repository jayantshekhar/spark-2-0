package org.workshop.dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class FlightsDataframe {

    public static void main(String[] args) throws org.apache.spark.sql.AnalysisException {

        SparkSession spark = SparkSession.builder().master("local").appName("FlightData").config("spark.some.config.option", "some-value")
                .getOrCreate();

        createFlightsDataframeUsingSchema(spark);

        Dataset<Row> airports = createAirportsDataframe(spark);

        Dataset<Row> flights = createFlightsDataframe(spark);

        counts(spark, airports, flights);

        joinAndSelect(spark, airports, flights);
    }

    public static void counts(SparkSession spark, Dataset<Row> airports, Dataset<Row> flights) {
        flights.createOrReplaceTempView("flights");

        // count of distinct tail nums
        Dataset<Row> distinctTailNums = flights.sqlContext().sql("SELECT COUNT(DISTINCT TAIL_NUM) from flights");
        distinctTailNums.show();

        // number of flights per tailnum
        Dataset<Row> numFlightsPerTailNum = flights.groupBy("TAIL_NUM").count();
        numFlightsPerTailNum.show();

    }

    public static void createFlightsDataframeUsingSchema(SparkSession spark) {
        // $example on:programmatic_schema$
        // Create an RDD
        JavaRDD<String> flightsRDD = spark.sparkContext()
                .textFile("data/flights_data_noheader.csv", 1)
                .toJavaRDD();

        // The schema is encoded in a string
        //String schemaString = "DAY_OF_MONTH DAY_OF_WEEK CARRIER TAIL_NUM FL_NUM ORIGIN_AIRPORT_ID ORIGIN DEST_AIRPORT_ID DEST CRS_DEP_TIME DEP_TIME DEP_DELAY_NEW CRS_ARR_TIME ARR_TIME ARR_DELAY_NEW CRS_ELAPSED_TIME DISTANCE";
        String schemaString = "DAY_OF_MONTH DAY_OF_WEEK";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = flightsRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String record) throws Exception {
                String[] attributes = record.split(",");
                return RowFactory.create(attributes[0], attributes[1].trim());
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        df.show();

    }

    public static void joinAndSelect(SparkSession spark, Dataset<Row> airports, Dataset<Row> flights) {

        Dataset<Row> joinDataframe = flights.join(airports, "ORIGIN");
        joinDataframe.show();

        // Creates a temporary view using the DataFrame
        joinDataframe.createOrReplaceTempView("flights");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT ORIGIN, TAIL_NUM, _c1  FROM flights");
        results.show();

        results = joinDataframe.select("ORIGIN", "TAIL_NUM", "_c1");
        results.show();

    }

    public static Dataset<Row> createAirportsDataframe(SparkSession spark) {

        Dataset<Row> airports = spark.read().option("header", "false").csv("data/airport_codes.csv");
        airports = airports.withColumnRenamed("_c4", "ORIGIN");

        return airports;
    }

    public static Dataset<Row> createFlightsDataframe(SparkSession spark) {

        Dataset<Row> flights = spark.read().option("header", "true").csv("data/flights_data.csv");

        return flights;
    }

}