/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import uconn.cse.cassperf.hectorcassandraclient.InsertRowsForDataCF;

/**
 * 
 * @author nhannq
 */
public class ReadData {

	public void generateDataforCassandra(int uID, int noOfReplicas,
			int noOfSamples, int rate) { // to measure Cassandra replicating
											// performance
		int tsID = 0;
		long executedTime = 0;
		// String test = "";
		try {
			DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
			Date date = new Date();

			InsertRowsForDataCF iRFCF = new InsertRowsForDataCF();

			Double Value = 0.0;

			int minute = noOfSamples / (rate * 60);
			// System.out.println(rate);
			// System.out.println(noOfSamples);
			// System.out.println(minute);
			long firstStartTime = System.currentTimeMillis();
			// double[] values = new double[rate];
			// for (int i = 0; i < rate; i++) {
			// values[i] = 1;
			// }

			if (rate > 1000) {
				// System.out.println("Here");
				String startTime = dateFormat.format(date); // get the time when
				// sensor starts to put
				// data to the backend
				System.out.println("startTime-" + startTime);
				while (tsID < noOfSamples - rate + 1) {
					// long timeStart2 = System.currentTimeMillis();
					// Random randomGenerator = new Random();
					// for (int i = 0; i < rate; i++) { //comment if need to use
					// the execute2 function
					long timeStart = System.currentTimeMillis();
					// while (System.currentTimeMillis() - timeStart < 1000 /
					// rate) {
					// }
					// System.out.println(timeStart + " " + currentTime + " " +
					// 1.0*1000 / rate);
					// System.out.println(1.0*(currentTime - timeStart) + " " +
					// (1.0)*1000 / rate);
					// tsID++;

					// Value = 1.0;
					// iRFCF.execute(uID, tsID++);
					// create an array of values then put the whole array to the
					// backend
					executedTime += iRFCF.executeMultiColumns(uID, tsID, rate);
					// test += tsID;
					tsID += rate;
					if ((System.currentTimeMillis() - timeStart) < 1000)
						Thread.sleep(1000 - (System.currentTimeMillis() - timeStart));
					// end of this module
					// }
					// System.out.println("Took " + (System.currentTimeMillis()
					// - timeStart2));

					// System.out.println(tsID + " " + Value);
					// if (tsID % 1000 == 0) {
					// System.out.println(Value);
					// // break;
					// }

					// if (tsID == end) {
					// break;
					// }
					//
					if ((System.currentTimeMillis() - firstStartTime) > minute * 60 * 1000) {
						System.out.println("Finish putting " + tsID + " "
								+ noOfSamples + " " + rate + " in "
								+ executedTime + " microSec");
						System.out.println("rate:" + rate);
						System.out.println("drop:" + (noOfSamples - tsID));
						System.out.println("ratio:" + 1.0*(tsID / noOfSamples));
						break;
					}
				}
			} else {
				while (tsID < noOfSamples) {
					long timeStart = System.currentTimeMillis();
					// Random randomGenerator = new Random();
					for (int i = 0; i < rate; i++) {

						// Thread.sleep(1000 / rate);
						// Value = 1.0;
						iRFCF.executeOneColumn(uID, tsID++);
					}
					// long timeStart2 = System.currentTimeMillis();
					while ((System.currentTimeMillis() - timeStart) < 1000) {
					}
					// System.out.println("Took " + (System.currentTimeMillis()
					// - timeStart));

					// System.out.println(tsID + " " + Value);
					// if (tsID % 1000 == 0) {
					// System.out.println(Value);
					// // break;
					// }

					// if (tsID == end) {
					// break;
					// }
					//
					if ((System.currentTimeMillis() - firstStartTime) > minute * 60 * 1000) {
						System.out.println("Finish putting " + tsID + " "
								+ noOfSamples + " " + rate + " in "
								+ executedTime + " microSec");
						break;
					}
				}
			}
			// Close the input stream
			// System.out.println("Length string is " + test.length());
			System.out.println("Finish putting " + tsID + " " + noOfSamples
					+ " " + rate + " in " + executedTime + " microSec");
			System.out.println("rate:" + rate);
			System.out.println("drop:" + (noOfSamples - tsID));
			System.out.println("ratio:" + 1.0*(tsID / noOfSamples));
		} catch (Exception e) {// Catch exception if any
			System.out.println("Finish putting " + tsID + " " + noOfSamples
					+ " " + rate + " in " + executedTime + " microSec");
			System.out.println("rate:" + rate);
			System.out.println("drop:" + (noOfSamples - tsID));
			System.out.println("ratio:" + 1.0*(tsID / noOfSamples));
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
