package org.workshop.dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.workshop.Airport;

import java.util.ArrayList;
import java.util.List;

public class FlightsDataframe {

    public static void main(String[] args) throws org.apache.spark.sql.AnalysisException {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession.builder().master("local").appName("FlightData").config("spark.some.config.option", "some-value")
                .getOrCreate();

        createFlightsDataframeUsingSchema(spark);

        Dataset<Row> airports = createAirportsDataframe(spark);

        Dataset<Row> flights = createFlightsDataframe(spark);

        saveAndRead(spark, flights);

        dataframeToDataset(spark, airports, flights);

        filter(spark, airports, flights);

        counts(spark, airports, flights);

        joinAndSelect(spark, airports, flights);

    }

    // count the number of distinct tail nums and number of flights per tailnum
    public static void counts(SparkSession spark, Dataset<Row> airports, Dataset<Row> flights) {
        flights.createOrReplaceTempView("flights");

        // count of distinct tail nums
        Dataset<Row> distinctTailNums = flights.sqlContext().sql("SELECT COUNT(DISTINCT TAIL_NUM) from flights");
        distinctTailNums.show();

        // number of flights per tailnum
        Dataset<Row> numFlightsPerTailNum = flights.groupBy("TAIL_NUM").count();
        numFlightsPerTailNum.show();

    }

    // create flights dataframe by specifying the schema
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

    // join the 2 dataframes and perform a query on it
    public static void joinAndSelect(SparkSession spark, Dataset<Row> airports, Dataset<Row> flights) {

        Dataset<Row> joinDataframe = flights.join(airports, flights.col("ORIGIN").equalTo(airports.col("IATA")));
        joinDataframe.show();

        // Creates a temporary view using the DataFrame
        joinDataframe.createOrReplaceTempView("flights");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT ORIGIN, TAIL_NUM, IATA  FROM flights");
        results.show();

        results = joinDataframe.select("ORIGIN", "TAIL_NUM", "IATA");
        results.show();

    }

    // filter records
    public static void filter(SparkSession spark, Dataset<Row> airports, Dataset<Row> flights) {

        // filter records for CARRIER == AA
        Dataset<Row> filtered = flights.filter("CARRIER == 'AA'");
        filtered.show();

        filtered = flights.filter("DEP_DELAY_NEW > 50");
        filtered.show();
    }

    // create airport dataframe
    public static Dataset<Row> createAirportsDataframe(SparkSession spark) {

        Dataset<Row> airports = spark.read().option("header", "false").csv("data/airport_codes.csv");
        airports.printSchema();

        airports = airports.withColumnRenamed("_c0", "airportId");
        airports = airports.withColumnRenamed("_c1", "name");
        airports = airports.withColumnRenamed("_c2", "city");
        airports = airports.withColumnRenamed("_c3", "country");
        airports = airports.withColumnRenamed("_c4", "IATA");
        airports = airports.withColumnRenamed("_c5", "ICAO");
        airports = airports.withColumnRenamed("_c6", "latitude");
        airports = airports.withColumnRenamed("_c7", "longitude");

        return airports;
    }

    // create flights dataframe
    public static Dataset<Row> createFlightsDataframe(SparkSession spark) {

        Dataset<Row> flights = spark.read().option("header", "true").csv("data/flights_data.csv");

        return flights;
    }

    // convert dataframe to dataset
    public static void dataframeToDataset(SparkSession spark, Dataset<Row> airports, Dataset<Row> flights) {

        Encoder<Airport> airportEncoder = Encoders.bean(Airport.class);
        Dataset<Airport> result = airports.as(airportEncoder);

        List<Airport> resultList = result.collectAsList();

        int i = 0;
        for (Airport airport : resultList) {
            System.out.println(airport.toString());

            if (i > 10)
                break;
            i++;
        }

    }

    // save the dataframe as parquet files and read them back
    public static void saveAndRead(SparkSession spark, Dataset<Row> flights) {
        flights.show();
        flights
                .write()
                .partitionBy("CARRIER")
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save("CARRIER.parquet");

        flights.printSchema();

        Dataset<Row> parquetFileDF = spark.read().parquet("CARRIER.parquet");
        parquetFileDF.printSchema();
        parquetFileDF.show(200);

    }
}