package org.workshop;

import java.io.Serializable;

/**
 * Created by jayant on 12/8/17.
 */
public class Airport implements Serializable {

    public Airport(String code) {
        this.code = code;
    }
    public String code;
}
