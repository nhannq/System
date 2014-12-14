/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * convert a string of time series data into an array of time series each
 * element is separated by ";"
 *
 * @author nhannguyen
 */
//checked
public class Deserialize {

    public Deserialize() {
    }

    public List<Double> execute(String source) {
        List<Double> result = new ArrayList();
        String[] temp = source.split(";");
        for (int i = 0; i < temp.length; i++) {
            result.add(Double.parseDouble(temp[i]));
        }
        return result;
    }

    public static Deserialize getInstance() {
        return new Deserialize();
    }
}
