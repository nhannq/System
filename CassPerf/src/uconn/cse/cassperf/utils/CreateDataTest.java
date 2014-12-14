/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

import uconn.cse.cassperf.hectorcassandraclient.InsertRowsForDataCF;

/**
 *
 * @author nhannguyen
 */
public class CreateDataTest {

    public void readData(String filename) {
        try {
            InsertRowsForDataCF iRFCF = new InsertRowsForDataCF();
//            for (int i = 0; i < 15; i++) {
//                iRFCF.execute("Data", 101L, i, i);
//            }
//            for (int i = 0; i < 3; i++) {
//                iRFCF.execute("Data", 101L, 15 + i, i);
//            }
//            for (int i = 0; i < 3; i++) {
//                iRFCF.execute("Data", 101L, 18 + i, i);
//            }
        } catch (Exception e) {//Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
    }
}
