/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

/**
 *
 * @author nhannq
 */
public class TimeStampConverter {

    public long toTimeStamp(String datetime) {
        try {
            //test
            String test = "2012-09-09 19:58:15.158";
            java.sql.Timestamp ts = java.sql.Timestamp.valueOf(datetime);
            long tsTime = ts.getTime();
            System.out.println(tsTime);
            return tsTime;
        } catch (Exception ex) {
        }
        return 0;
    }

    public static TimeStampConverter getInstance() {
        return new TimeStampConverter();
    }
}
