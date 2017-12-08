package org.workshop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Flights {

    public static void main(String[] args) throws  org.apache.spark.sql.AnalysisException {

        //final SparkConf sparkConf = new SparkConf().setAppName("FlightData").setMaster("local");
        //JavaSparkContext sc = new JavaSparkContext(sparkConf) ;

        //RDD of flight data
        //JavaRDD<String> flightRdd = sc.textFile("data/flights_data.csv");
        //SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        SparkSession spark = SparkSession.builder().master("local").appName("FlightData").config("spark.some.config.option", "some-value")
                .getOrCreate();

        rdd1(spark);
        //dataframe(spark);
    }

    public static void rdd1(SparkSession spark) {
        JavaRDD<Flight> flights = createRDD(spark, "data/flights_data_noheader.csv");

        JavaRDD<Airport> airports = createRDD1(spark, "data/airport_codes.csv");

        // count
        long numOrigin = flights.map(new Function<Flight, String>() {
            @Override
            public String call(Flight r) {
                return r.origin;
            }
        }).distinct().count();

        System.out.println("Number of origins : " + numOrigin);

        JavaPairRDD<String, Integer> ones = flights.mapToPair(
                new PairFunction<Flight, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Flight s) {
                        return new Tuple2<>(s.origin, 1);
                    }
                });


        // count
        long numAirports = airports.map(new Function<Airport, String>() {
            @Override
            public String call(Airport r) {
                return r.code;
            }
        }).distinct().count();

        System.out.println("Number of airports : " + numOrigin);


    }

    public static void dataframe1(SparkSession spark) {
        // $example on:programmatic_schema$
        // Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("data/flights_data.csv", 1)
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

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map(new Function<String, Row>() {
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

    public static void dataframe(SparkSession spark) {

        //create dataframe from csv file(flight_data)
        Dataset<Row> flightDataframe = spark.read().option("header","true").csv("data/flights_data.csv");

        flightDataframe.show();

        Dataset<Row> airportCodesDataframe = spark.read().option("header","false").csv("data/airport_codes.csv");

        airportCodesDataframe.show();
        airportCodesDataframe = airportCodesDataframe.withColumnRenamed("_c4", "ORIGIN");
        Dataset<Row> joinDataframe = flightDataframe.join(airportCodesDataframe, "ORIGIN");
        joinDataframe.show();


        // Creates a temporary view using the DataFrame
        joinDataframe.createOrReplaceTempView("flights");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT ORIGIN FROM flights");
        results.show();

        results = joinDataframe.select("ORIGIN");
        results.show();

    }

    public static JavaRDD<Flight> createRDD(SparkSession sparkSession, String inputFile) {

        // create RDD
        JavaRDD<String> lines = sparkSession.sparkContext().textFile(inputFile, 1).toJavaRDD();;

        JavaRDD<Flight> flights = lines.map(new Function<String, Flight>() {
            @Override
            public Flight call(String s) throws Exception {
                String[] arr = s.split(",");

                // user::movie::rating
                return new Flight(arr[0]);
            }
        });

        return flights;
    }

    public static JavaRDD<Airport> createRDD1(SparkSession sparkSession, String inputFile) {

        // create RDD
        JavaRDD<String> lines = sparkSession.sparkContext().textFile(inputFile, 1).toJavaRDD();;

        JavaRDD<Airport> airports = lines.map(new Function<String, Airport>() {
            @Override
            public Airport call(String s) throws Exception {
                String[] arr = s.split(",");

                // user::movie::rating
                return new Airport(arr[0]);
            }
        });

        return airports;
    }


}
