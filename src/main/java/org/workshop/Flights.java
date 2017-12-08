package org.workshop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class Flights {

    public static void main(String[] args) throws  org.apache.spark.sql.AnalysisException{

        final SparkConf sparkConf = new SparkConf().setAppName("FlightData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf) ;
        //RDD of flight data
        JavaRDD<String> flightRdd = sc.textFile("D:/Projects/SPARK_2_0/spark-2-0/data/flights_data.csv");
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        SparkSession spark = SparkSession.builder().master("local").appName("FlightData").config("spark.some.config.option", "some-value")
                .getOrCreate();
        //create dataframe from csv file(flight_data)
        Dataset<Row> flightDataframe = spark.read().option("header","true").csv("data/flights_data.csv");

        flightDataframe.show();

        Dataset<Row> airportCodesDataframe = spark.read().option("header","false").csv("data/airport_codes.csv");

        airportCodesDataframe.show();
        airportCodesDataframe = airportCodesDataframe.withColumnRenamed("_c4", "ORIGIN");
        Dataset<Row> joinDataframe = flightDataframe.join(airportCodesDataframe, "ORIGIN");
        joinDataframe.show();




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
}
