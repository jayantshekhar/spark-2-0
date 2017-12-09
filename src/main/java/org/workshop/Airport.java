package org.workshop;

import java.io.Serializable;

/**
 * Created by jayant on 12/8/17.
 */

// https://openflights.org/data.html
/*
Airport ID	Unique OpenFlights identifier for this airport.
Name	Name of airport. May or may not contain the City name.
City	Main city served by airport. May be spelled differently from Name.
Country	Country or territory where airport is located. See countries.dat to cross-reference to ISO 3166-1 codes.
IATA	3-letter IATA code. Null if not assigned/unknown.
ICAO	4-letter ICAO code.
Null if not assigned.
Latitude	Decimal degrees, usually to six significant digits. Negative is South, positive is North.
Longitude	Decimal degrees, usually to six significant digits. Negative is West, positive is East.
Altitude	In feet.
Timezone	Hours offset from UTC. Fractional hours are expressed as decimals, eg. India is 5.5.
DST	Daylight savings time. One of E (Europe), A (US/Canada), S (South America), O (Australia), Z (New Zealand), N (None) or U (Unknown). See also: Help: Time
Tz database time zone	Timezone in "tz" (Olson) format, eg. "America/Los_Angeles".
Type	Type of the airport. Value "airport" for air terminals, "station" for train stations, "port" for ferry terminals and "unknown" if not known. In airports.csv, only type=airport is included.
Source	Source of this data. "OurAirports" for data sourced from OurAirports, "Legacy" for old data not matched to OurAirports (mostly DAFIF), "User" for unverified user contributions. In airports.csv, only source=OurAirports is included.
     */
public class Airport implements Serializable {

    public String airportId;
    public String name;
    public String city;
    public String country;
    public String IATA;
    public String ICAO;
    public String latitude;
    public String longitude;
    public String altitude;
    public String timezone;
    public String DST;
    public String tzDatabase;
    public String type;
    public String source;

    public Airport(String[] arr) {

        for (int i=0; i<arr.length; i++) {
            arr[i] = arr[i].replace('"', ' ').trim();
        }

        this.airportId = arr[0];
        this.name = arr[1];
        this.city = arr[2];
        this.country = arr[3];
        this.IATA = arr[4];
        this.ICAO = arr[5];
        this.latitude = arr[6];
        this.longitude = arr[7];
        this.altitude = arr[8];
        this.timezone = arr[9];
        this.DST = arr[10];
        this.tzDatabase = arr[11];
        this.type = arr[12];
        this.source = arr[13];
    }

    public String toString() {
        return " IATA : " + IATA +
                " name : " + name;

    }
}
