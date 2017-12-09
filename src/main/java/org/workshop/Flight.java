package org.workshop;

import java.io.Serializable;

/**
 * Created by jayant on 12/8/17.
 */
public class Flight implements Serializable{

    public String DAY_OF_MONTH,DAY_OF_WEEK,CARRIER,TAIL_NUM,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN,DEST_AIRPORT_ID,
            DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY_NEW,
            CRS_ARR_TIME,ARR_TIME,ARR_DELAY_NEW,CRS_ELAPSED_TIME;

    public long DISTANCE;

    public Flight(String[] arr) {
        this.DAY_OF_MONTH = arr[0];
        this.DAY_OF_WEEK = arr[1];
        this.CARRIER = arr[2];
        this.TAIL_NUM = arr[3];
        this.FL_NUM = arr[4];
        this.ORIGIN_AIRPORT_ID = arr[5];
        this.ORIGIN = arr[6];
        this.DEST_AIRPORT_ID = arr[7];
        this.DEST = arr[8];
        this.CRS_DEP_TIME = arr[9];
        this.DEP_TIME = arr[10];
        this.DEP_DELAY_NEW = arr[11];
        this.CRS_ARR_TIME = arr[12];
        this.ARR_TIME = arr[13];
        this.ARR_DELAY_NEW = arr[14];
        this.CRS_ELAPSED_TIME = arr[15];
        this.DISTANCE = Long.parseLong(arr[16]);
    }

    public String toString() {
        return " CARRIER : " + CARRIER +
                " ORIGIN : " + ORIGIN;

    }

}
