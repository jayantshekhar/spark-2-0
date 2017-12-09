package org.workshop;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

public class FlightsRDD {

    public static void main(String[] args) throws org.apache.spark.sql.AnalysisException {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession.builder().master("local").appName("FlightData").config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaRDD<Airport> airports = createAirportsRDD(spark, "data/airport_codes.csv");

        JavaRDD<Flight> flights = createFlightsRDD(spark, "data/flights_data_noheader.csv");

        counts(spark, airports, flights);

        displayRecords(spark, airports, flights);

        distance(spark, flights);

        join(spark, airports, flights);
    }

    public static void counts(SparkSession spark, JavaRDD<Airport> airports,
                              JavaRDD<Flight> flights) {
        System.out.println("Number of Flights : " + flights.count());

        // count
        long numCARRIER = flights.map(new Function<Flight, String>() {
            @Override
            public String call(Flight r) {
                return r.CARRIER;
            }
        }).distinct().count();

        System.out.println("Number of CARRIERS : " + numCARRIER);

        JavaPairRDD<String, Integer> ones = flights.mapToPair(
                new PairFunction<Flight, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Flight s) {
                        return new Tuple2<>(s.CARRIER, 1);
                    }
                });


        //----------------

        System.out.println("Number of airports : " + airports.count());

        // count
        long numAirports = airports.map(new Function<Airport, String>() {
            @Override
            public String call(Airport r) {
                return r.ICAO;
            }
        }).distinct().count();

        System.out.println("Number of airports : " + numAirports);


    }

    public static JavaRDD<Flight> createFlightsRDD(SparkSession sparkSession, String inputFile) {

        // create RDD
        JavaRDD<String> lines = sparkSession.sparkContext().textFile(inputFile, 1).toJavaRDD();
        ;

        JavaRDD<Flight> flights = lines.map(new Function<String, Flight>() {
            @Override
            public Flight call(String s) throws Exception {
                String[] arr = s.split(",");

                // user::movie::rating
                return new Flight(arr);
            }
        });

        return flights;
    }

    public static JavaRDD<Airport> createAirportsRDD(SparkSession sparkSession, String inputFile) {

        // create RDD
        JavaRDD<String> lines = sparkSession.sparkContext().textFile(inputFile, 1).toJavaRDD();
        ;

        JavaRDD<Airport> airports = lines.map(new Function<String, Airport>() {
            @Override
            public Airport call(String s) throws Exception {
                String[] arr = s.split(",");

                return new Airport(arr);
            }
        });

        return airports;
    }

    public static void displayRecords(SparkSession spark, JavaRDD<Airport> airports,
                                      JavaRDD<Flight> flights) {

        List<Airport> airportList = airports.collect();

        int i = 0;
        for (Airport airport : airportList) {
            System.out.println(airport.toString());

            if (i > 10)
                break;
            i++;
        }

        List<Flight> flightList = flights.collect();

        i = 0;
        for (Flight flight : flightList) {
            System.out.println(flight.toString());

            if (i > 10)
                break;
            i++;
        }

    }

    public static void join(SparkSession spark, JavaRDD<Airport> airports,
                                      JavaRDD<Flight> flights) {

        JavaPairRDD<String, Airport> airportPair = airports.mapToPair(
                airport -> new Tuple2<>(airport.IATA, airport));

        /***
        JavaPairRDD<String, Airport> airportPair = airports.mapToPair(
                new PairFunction<Airport, String, Airport>() {
                    @Override
                    public Tuple2<String, Airport> call(Airport airport) {
                        return new Tuple2<>(airport.IATA, airport);
                    }
                });
         ***/

        JavaPairRDD<String, Flight> flightPair = flights.mapToPair(
                new PairFunction<Flight, String, Flight>() {
                    @Override
                    public Tuple2<String, Flight> call(Flight flight) {
                        return new Tuple2<>(flight.ORIGIN, flight);
                    }
                });

        JavaPairRDD<String, Tuple2<Flight, Airport>> flightAirportPair = flightPair.join(airportPair);

        List<Tuple2<Flight,Airport>> flightAirportList = flightAirportPair.values().collect();

        int i = 0;
        for (Tuple2<Flight,Airport> tuple : flightAirportList) {
            System.out.println(tuple._1().CARRIER + " : "+ tuple._1().ORIGIN + " : " + tuple._2().name);

            if (i > 10)
                break;
            i++;
        }
    }

    public static void distance(SparkSession spark,
                            JavaRDD<Flight> flights) {

        JavaPairRDD<String, Long> flightPair = flights.mapToPair(
                flight -> new Tuple2<>(flight.TAIL_NUM, flight.DISTANCE));

        JavaPairRDD<String, Long> distanceByTailNum = flightPair.reduceByKey((a, b) -> a+b);

        List<Tuple2<String,Long>> totalDistanceByTailNum = distanceByTailNum.collect();

        int i = 0;
        for (Tuple2<String,Long> tuple : totalDistanceByTailNum) {
            System.out.println(tuple._1() + " : "+ tuple._2());

            if (i > 10)
                break;
            i++;
        }
    }

}