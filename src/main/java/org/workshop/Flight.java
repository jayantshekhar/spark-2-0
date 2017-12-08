package org.workshop;

import java.io.Serializable;

/**
 * Created by jayant on 12/8/17.
 */
public class Flight implements Serializable{

    public Flight(String origin) {
        this.origin = origin;
    }
    public String origin;
}
