package org.workshop;

import java.io.Serializable;

/**
 * Created by jayant on 12/8/17.
 */

/***
 DAY_OF_MONTH : 1
 DAY_OF_WEEK : 3
 CARRIER : AA
 TAIL_NUM : N338AA
 FL_NUM : 1
 ORIGIN_AIRPORT_ID : 12478
 ORIGIN : JFK
 DEST_AIRPORT_ID : 12892
 DEST : LAX
 CRS_DEP_TIME : 900
 DEP_TIME : 914
 DEP_DELAY_NEW : 14
 CRS_ARR_TIME : 1225
 ARR_TIME : 1238
 ARR_DELAY_NEW : 13
 CRS_ELAPSED_TIME : 385
 DISTANCE : 2475
***/

public class Flight implements Serializable{

    public String DAY_OF_MONTH,DAY_OF_WEEK,CARRIER,TAIL_NUM,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN,DEST_AIRPORT_ID,
            DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY_NEW,
            CRS_ARR_TIME,ARR_TIME,ARR_DELAY_NEW,CRS_ELAPSED_TIME;

    public void setDAY_OF_MONTH(String DAY_OF_MONTH) {
        this.DAY_OF_MONTH = DAY_OF_MONTH;
    }

    public String getFL_NUM() {
        return FL_NUM;
    }

    public String getORIGIN_AIRPORT_ID() {
        return ORIGIN_AIRPORT_ID;
    }

    public String getDEST_AIRPORT_ID() {
        return DEST_AIRPORT_ID;
    }

    public String getCRS_DEP_TIME() {
        return CRS_DEP_TIME;
    }

    public String getDEP_TIME() {
        return DEP_TIME;
    }

    public String getCRS_ARR_TIME() {
        return CRS_ARR_TIME;
    }

    public String getARR_TIME() {
        return ARR_TIME;
    }

    public long getDISTANCE() {
        return DISTANCE;
    }

    public String getDAY_OF_MONTH() {
        return DAY_OF_MONTH;
    }

    public void setDAY_OF_WEEK(String DAY_OF_WEEK) {
        this.DAY_OF_WEEK = DAY_OF_WEEK;
    }
    public String getDAY_OF_WEEK() {
        return DAY_OF_WEEK;
    }

    public void setCARRIER(String CARRIER) {
        this.CARRIER = CARRIER;
    }
    public String getCARRIER() {
        return CARRIER;
    }

    public void setTAIL_NUM(String TAIL_NUM) {
        this.TAIL_NUM = TAIL_NUM;
    }
    public String getTAIL_NUM() {
        return TAIL_NUM;
    }

    public void setORIGIN(String ORIGIN) {
        this.ORIGIN = ORIGIN;
    }
    public String getORIGIN() {
        return ORIGIN;
    }

    public void setDEST(String DEST) {
        this.DEST = DEST;
    }
    public String getDEST() {
        return DEST;
    }

    public void setDEP_DELAY_NEW(String DEP_DELAY_NEW) {
        this.DEP_DELAY_NEW = DEP_DELAY_NEW;
    }
    public String getDEP_DELAY_NEW() {
        return DEP_DELAY_NEW;
    }

    public void setARR_DELAY_NEW(String ARR_DELAY_NEW) {
        this.ARR_DELAY_NEW = ARR_DELAY_NEW;
    }
    public String getARR_DELAY_NEW() {
        return ARR_DELAY_NEW;
    }

    public void setCRS_ELAPSED_TIME(String CRS_ELAPSED_TIME) {
        this.CRS_ELAPSED_TIME = CRS_ELAPSED_TIME;
    }
    public String getCRS_ELAPSED_TIME() {
        return CRS_ELAPSED_TIME;
    }

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

    public void setFL_NUM(String FL_NUM) {
        this.FL_NUM = FL_NUM;
    }

    public void setORIGIN_AIRPORT_ID(String ORIGIN_AIRPORT_ID) {
        this.ORIGIN_AIRPORT_ID = ORIGIN_AIRPORT_ID;
    }

    public void setDEST_AIRPORT_ID(String DEST_AIRPORT_ID) {
        this.DEST_AIRPORT_ID = DEST_AIRPORT_ID;
    }

    public void setCRS_DEP_TIME(String CRS_DEP_TIME) {
        this.CRS_DEP_TIME = CRS_DEP_TIME;
    }

    public void setDEP_TIME(String DEP_TIME) {
        this.DEP_TIME = DEP_TIME;
    }

    public void setCRS_ARR_TIME(String CRS_ARR_TIME) {
        this.CRS_ARR_TIME = CRS_ARR_TIME;
    }

    public void setARR_TIME(String ARR_TIME) {
        this.ARR_TIME = ARR_TIME;
    }

    public void setDISTANCE(long DISTANCE) {
        this.DISTANCE = DISTANCE;
    }
}
