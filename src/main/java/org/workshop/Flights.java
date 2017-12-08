package org.workshop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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

        rdd(spark);
        //dataframe(spark);
    }

    public static void rdd1(SparkSession spark) {
        JavaRDD<Flight> flights = createRDD(spark, "data/flights_data_noheader.csv");

    }

    public static void rdd(SparkSession spark) {
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


        // Convert records of the RDD (people) to Rows
	/* JavaRDD<Row> rowRDD = flightRdd.map(new Function<String, Row>() {
	   public Row call(String record) throws Exception {
	     String[] attributes = record.split(",");
	     return RowFactory.create(attributes[0], attributes[1], attributes[2], attributes[3], attributes[4]
	    		 , attributes[5], attributes[6], attributes[7], attributes[8], attributes[9], attributes[10]
	    				 , attributes[11], attributes[12], attributes[13], attributes[14], attributes[15], attributes[16].trim());
	   }
	 });
	*/

        // The schema is encoded in a string
	/* String schemaString = "field1 field2 field3 field4 field5 field6 field7 field8 field9 field10 field11 field12 field13 field14 field15 field16 field17";

	 // Generate the schema based on the string of schema
	 List<StructField> fields = new ArrayList<>();

	 for (String fieldName : schemaString.split(" ")) {
	   StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
	   fields.add(field);
	 }
	 StructType schema = DataTypes.createStructType(fields);

	// Apply the schema to the RDD.
			 Dataset<Row> flightsDataFrame = sqlContext.createDataFrame(rowRDD, schema);

			 // Register the DataFrame as a table.
			 flightsDataFrame.registerTempTable("flight_details");

			 // SQL can be run over RDDs that have been registered as tables.
			 Dataset<Row> results = sqlContext.sql("SELECT * FROM flight_details");
			 results.show();*/


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

}
