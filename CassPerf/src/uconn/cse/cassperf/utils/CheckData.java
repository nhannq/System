/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;
import uconn.cse.cassperf.hectorcassandraclient.GetDataFromDataCF;
import uconn.cse.cassperf.hectorcassandraclient.InsertRowsForDataCF;

/**
 * 
 * @author nhannguyen - uconn
 */
public class CheckData {

	public void checkDatafromCassandra(int uID, int noOfReplicas,
			int noOfSamples, String lcheckfile, int rate, int delayTime, String logFileName) { // to
																			// measure
		// Cassandra
		// replicating
		// performance
		Double sum = 0.0;
		int minute = noOfSamples / (rate * 60) + delayTime;
		int[] samplesPerMinute = new int[minute];
		for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
			samplesPerMinute[minuteIdx] = 0;
		try {

			GetDataFromDataCF iGFCF = new GetDataFromDataCF();
			int tsID = 0;
			// DateFormat dateFormat = new
			// SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
			// Date date = new Date();
			String startTime = ""; // get the time when
			// sensor starts to put
			// data to the backend
			BufferedReader br = new BufferedReader(new FileReader(logFileName));
			try {
				StringBuilder sb = new StringBuilder();
				String line = br.readLine();

				while (line != null) {
					if (line.contains("startTime")) {
						startTime = line.split("-")[1];
					}

					line = br.readLine();
				}
				String everything = sb.toString();
			} finally {
				br.close();
			}

			String[] parsedStartTime = startTime.split("\\s+");
			System.out.println(parsedStartTime[0]);
			System.out.println(parsedStartTime[1]);
			int startHour = Integer.parseInt(parsedStartTime[1].split(":")[0]);
			int startMinute = Integer
					.parseInt(parsedStartTime[1].split(":")[1]);
			int startSecond = Integer
					.parseInt(parsedStartTime[1].split(":")[2]);
			String putTime;
			int putHour = 0;
			int putMinute = 0;
			int putSecond = 0;
			String[] parsedPutTime;

			if (startMinute >= 54 || startMinute <= 59) {
				// startMinute = startMinute - 60;
				while (tsID < noOfSamples - rate + 1) {
					QueryResult<ColumnSlice<Integer, Double>> result0 = iGFCF
							.execute("Data", uID, tsID, tsID + rate - 1, false,
									rate);
					ColumnSlice<Integer, Double> colslice0 = result0.get(); // get
																			// data
																			// from
																			// raw
																			// data
																			// table
					List<HColumn<Integer, Double>> dataColumns = colslice0
							.getColumns();
					if (dataColumns.size() > 0) {
						sum += colslice0.getColumns().size() * 1.0;
						// if (parsedStartTime[0].equals(parsedPutTime[0])) {
						// //if
						// the date of put time is similar to the date of start
						// time
						// System.out.println("Same day");
						// }
						// System.out.println(startTime);
						for (int columnIdx = 0; columnIdx < rate; columnIdx++) {
							putTime = new java.text.SimpleDateFormat(
									"MM/dd/yyyy HH:mm:ss")
									.format(new java.util.Date(dataColumns.get(
											columnIdx).getClock() / 1000));
							parsedPutTime = putTime.split("\\s+");
							// System.out.println(parsedPutTime[1]);
							// putHour =
							// Integer.parseInt(parsedPutTime[1].split(":")[0]);
							putMinute = Integer.parseInt(parsedPutTime[1]
									.split(":")[1]);
							if (putMinute <= 5)
								putMinute += 60;
							putSecond = Integer.parseInt(parsedPutTime[1]
									.split(":")[2]);
							for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++) {
								if ((putMinute == startMinute + minuteIdx && putSecond >= startSecond)
										|| (putMinute == startMinute
												+ minuteIdx + 1 && putSecond < startSecond)) {
									samplesPerMinute[minuteIdx]++;
								}
							}
						}
					}
					tsID += rate;
				}
			} else {
				while (tsID < noOfSamples - rate + 1) {
					QueryResult<ColumnSlice<Integer, Double>> result0 = iGFCF
							.execute("Data", uID, tsID, tsID + rate - 1, false,
									rate);
					ColumnSlice<Integer, Double> colslice0 = result0.get(); // get
																			// data
																			// from
																			// raw
																			// data
																			// table
					List<HColumn<Integer, Double>> dataColumns = colslice0
							.getColumns();
					if (dataColumns.size() > 0) {
						sum += dataColumns.size() * 1.0;
						// if (parsedStartTime[0].equals(parsedPutTime[0])) {
						// //if
						// the date of put time is similar to the date of start
						// time
						// System.out.println("Same day");
						// }
						// System.out.println(startTime);
						for (int columnIdx = 0; columnIdx < rate; columnIdx++) {
							putTime = new java.text.SimpleDateFormat(
									"MM/dd/yyyy HH:mm:ss")
									.format(new java.util.Date(dataColumns.get(
											columnIdx).getClock() / 1000));
							parsedPutTime = putTime.split("\\s+");
							// System.out.println(parsedPutTime[1]);
							// putHour =
							// Integer.parseInt(parsedPutTime[1].split(":")[0]);
							putMinute = Integer.parseInt(parsedPutTime[1]
									.split(":")[1]);
							putSecond = Integer.parseInt(parsedPutTime[1]
									.split(":")[2]);
							for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++) {
								if ((putMinute == startMinute + minuteIdx && putSecond >= startSecond)
										|| (putMinute == startMinute
												+ minuteIdx + 1 && putSecond < startSecond)) {
									samplesPerMinute[minuteIdx]++;
									break;
								}
							}
						}
					}
					tsID += rate;
				}
			}
			// for (int i = 0; i < colslice0.getColumns().size(); i++) {
			// sum += colslice0.getColumns().get(i).getValue();
			// }
			// Close the input stream
			// colslice0.getColumns().get(0).getName();
			// System.out.println("Last column");
			// System.out.println(colslice0.getColumns().get(0).getName());
			// System.out.println(colslice0.getColumns().get(0).getClock()/1000);
			// String startTime = new
			// java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new
			// java.util.Date (colslice0.getColumns().get(0).getClock()/1000));
			// System.out.println(startTime);
			// String startHour = startTime.split("\\s+")[1];
			// int startMinute = Integer.parseInt(startHour.split(":")[1]);
			// //get the minute of the first column
			// int startSecond = Integer.parseInt(startHour.split(":")[2]);
			// //get the minute of the first column
			// int count = 0;
			// for (int i = 0; i < colslice0.getColumns().size(); i++) {
			// String tempTime = new
			// java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new
			// java.util.Date (colslice0.getColumns().get(i).getClock()/1000));
			// // System.out.println(tempTime);
			// String tempHour = tempTime.split("\\s+")[1];
			// int tempMinute = Integer.parseInt(tempHour.split(":")[1]); //get
			// the minute of the first column
			// int tempSecond = Integer.parseInt(tempHour.split(":")[2]); //get
			// the minute of the first column
			// if (tempMinute == startMinute & tempSecond >= startSecond )
			// count++;
			// if (tempMinute == (startMinute + 1)%60 & tempSecond <=
			// startSecond)
			// count++;
			// }
			// System.out.println(startHour);
			// String endTime = new
			// java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new
			// java.util.Date
			// (colslice0.getColumns().get(colslice0.getColumns().size()-1).getClock()/1000));
			// String endHour = endTime.split("\\s+")[1];
			// System.out.println(colslice0.getColumns().get(colslice0.getColumns().size()-1).getClock()/1000);
			// System.out.println(endTime);

			System.out.println("Finish checking: " + tsID + " size " + sum
					+ " / " + noOfSamples + " : " + (sum / noOfSamples));
			System.out.println("rate:" + rate);
			System.out.println("drop:" + (noOfSamples - sum));
			System.out.println("ratio:" + sum / noOfSamples);
			for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
				System.out.println("minute " + minuteIdx + " : "
						+ samplesPerMinute[minuteIdx]);
		} catch (Exception e) {// Catch exception if any
			System.out.println("Finish checking: sum " + sum + " / "
					+ noOfSamples + " : " + (sum / noOfSamples));
			System.out.println("rate:" + rate);
			System.out.println("drop:" + (noOfSamples - sum));
			System.out.println("ratio:" + sum / noOfSamples);
			for (int minuteIdx = 0; minuteIdx < minute; minuteIdx++)
				System.out.println("minute " + minuteIdx + " : "
						+ samplesPerMinute[minuteIdx]);
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
