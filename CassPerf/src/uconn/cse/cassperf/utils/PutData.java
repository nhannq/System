/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author nhannq
 */
public class PutData {
  private static Logger logger = LoggerFactory.getLogger(PutData.class);

  // to measure Cassandra replicating performance
  public void generateDataforCassandraHector(int uID, int noOfReplicas, int minute, int rate) {
    int tsID = 0;
    long executedTime = 0;
    String timeStampOutput = "";
    int noOfSamples = minute * rate * 60;
    try {
      DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

      uconn.cse.cassperf.hectorcassandraclient.InsertRowsForDataCF iRFCF =
          new uconn.cse.cassperf.hectorcassandraclient.InsertRowsForDataCF();
      Double Value = 0.0;


      // System.out.println(rate);
      // System.out.println(noOfSamples);
      // System.out.println(minute);
      long firstStartTime = System.currentTimeMillis();
      // double[] values = new double[rate];
      // for (int i = 0; i < rate; i++) {
      // values[i] = 1;
      // }

      Date date = new Date();
      String startTime = dateFormat.format(date); // get the time when
      // sensor starts to put
      // data to the backend
      System.out.println("startTime-" + startTime);


      if (rate > 1000) {
        // System.out.println("Here");

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
          logger.info("ID : " + tsID + "-" + dateFormat.format(new Date()));
          timeStampOutput += "ID : " + tsID + "-" + dateFormat.format(new Date()) + "\n";
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
            System.out.println(timeStampOutput);
            System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
                + executedTime + " microSec");
            System.out.println("rate:" + rate);
            System.out.println("drop:" + (noOfSamples - tsID));
            System.out.println("ratio:" + tsID / noOfSamples);
            return;
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
            System.out.println(timeStampOutput);
            System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
                + executedTime + " microSec");
            System.out.println("rate:" + rate);
            System.out.println("drop:" + (noOfSamples - tsID));
            System.out.println("ratio:" + tsID / noOfSamples);
            break;
          }
        }
      }
      // Close the input stream
      // System.out.println("Length string is " + test.length());
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + executedTime + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
      System.out.println("finishTime ms " + System.currentTimeMillis());
    } catch (Exception e) {// Catch exception if any
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + executedTime + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
      System.err.println("Error: " + e.getMessage());
      System.out.println("finishTime ms " + System.currentTimeMillis());
      logger.debug("failed to write data to server", e);
      e.printStackTrace();
    }
  }

  public void generateDataforCassandraDatastax(int uID, int noOfReplicas, int noOfSamples, int rate) {
    int tsID = 0;
    long executedTime = 0;
    String timeStampOutput = "";
    // String test = "";
    try {
      DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
      uconn.cse.cassperf.datastaxcassandraclient.InsertRowsForDataCF iRFCF =
          new uconn.cse.cassperf.datastaxcassandraclient.InsertRowsForDataCF();
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
        Date date = new Date();
        String startTime = dateFormat.format(date); // get the time when
        // sensor starts to put
        // data to the backend
        System.out.println("startTime-" + startTime);

        while (tsID < noOfSamples - rate + 1) {
          long timeStart = System.currentTimeMillis();
          // for (int i = 0; i < rate; i++) {
          // iRFCF.executeOneColumn(uID, tsID);
          // tsID += 1;
          // }
          iRFCF.executeMultiColumns(uID, tsID, rate);
          tsID += rate;
          timeStampOutput += "ID : " + tsID + "-" + dateFormat.format(new Date()) + "\n";
          if ((System.currentTimeMillis() - timeStart) < 1000)
            Thread.sleep(1000 - (System.currentTimeMillis() - timeStart));

          if ((System.currentTimeMillis() - firstStartTime) > minute * 60 * 1000) {
            System.out.println(timeStampOutput);
            System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
                + executedTime + " microSec");
            System.out.println("rate:" + rate);
            System.out.println("drop:" + (noOfSamples - tsID));
            System.out.println("ratio:" + tsID / noOfSamples);
            return;
          }
        }
      }
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + executedTime + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
    } catch (Exception e) {// Catch exception if any
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + executedTime + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public void generateDataforCassandraAstyanax(int uID, int noOfReplicas, int noOfSamples, int rate) {
    int tsID = 0;
    long executedTime = 0;
    String timeStampOutput = "";
    // String test = "";
    try {
      DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
      uconn.cse.cassperf.astyanaxcassandraclient.InsertRowsForDataCF iRFCF =
          new uconn.cse.cassperf.astyanaxcassandraclient.InsertRowsForDataCF();
      Double Value = 0.0;
      iRFCF.executeMultiColumns(uID, 0, 10);
      System.out.println("Sleeping 10 seconds");
      Thread.sleep(10000);

      int minute = noOfSamples / (rate * 60);
      long timeStart;
      // System.out.println(rate);
      // System.out.println(noOfSamples);
      // System.out.println(minute);

      // double[] values = new double[rate];
      // for (int i = 0; i < rate; i++) {
      // values[i] = 1;
      // }
      long timeout = (minute * 60 + 1) * 1000;// 1 second delay at when
      // the sensor puts data to
      // the server
      if (rate > 1000) {
        // System.out.println("Here");
        Date date = new Date();
        String startTime = dateFormat.format(date); // get the time when
        // sensor starts to put
        // data to the backend
        timeStampOutput += "startTime-" + startTime + "\n";
        long firstStartTime = System.currentTimeMillis();
        while (tsID < noOfSamples - rate + 1) {
          timeStart = System.currentTimeMillis();
          // for (int i = 0; i < rate; i++) {
          // iRFCF.executeOneColumn(uID, tsID);
          // tsID += 1;
          // }
          iRFCF.executeMultiColumns(uID, tsID, rate);
          tsID += rate;
          logger.info("ID : " + tsID + "-" + dateFormat.format(new Date()));
          timeStampOutput += "ID : " + tsID + "-" + dateFormat.format(new Date()) + "\n";
          if ((System.currentTimeMillis() - timeStart) < 1000)
            Thread.sleep(1000 - (System.currentTimeMillis() - timeStart));

          if ((System.currentTimeMillis() - firstStartTime) > timeout) {
            System.out.println(timeStampOutput);
            System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
                + executedTime + " microSec");
            System.out.println("rate:" + rate);
            System.out.println("drop:" + (noOfSamples - tsID));
            System.out.println("ratio:" + tsID / noOfSamples);
            return;
          }
        }
      }
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + executedTime + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
    } catch (Exception e) {// Catch exception if any
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + executedTime + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
      System.err.println("Error: " + e.getMessage());
      logger.debug("failed to write data to server", e);
      e.printStackTrace();
    }
  }

  public void generateDataforCassandraAstyanaxOneClient(int uID, int noOfReplicas, int noOfSamples,
      int rate) {
    System.out.println("generateDataforCassandraAstyanaxOneClient");
    int tsID = 0;
    long startTime = System.currentTimeMillis();
    String timeStampOutput = "";
    // String test = "";
    try {
      uconn.cse.cassperf.astyanaxcassandraclient.InsertRowsForDataCF iRFCF =
          new uconn.cse.cassperf.astyanaxcassandraclient.InsertRowsForDataCF();

      for (tsID = 0; tsID < noOfSamples; tsID++) {
        iRFCF.executeMultiColumns(uID, tsID, rate);
      }

      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + (System.currentTimeMillis() - startTime) + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
    } catch (Exception e) {// Catch exception if any
      System.out.println(timeStampOutput);
      System.out.println("Finish putting " + tsID + " " + noOfSamples + " " + rate + " in "
          + (System.currentTimeMillis() - startTime) + " microSec");
      System.out.println("rate:" + rate);
      System.out.println("drop:" + (noOfSamples - tsID));
      System.out.println("ratio:" + tsID / noOfSamples);
      System.err.println("Error: " + e.getMessage());
      logger.debug("failed to write data to server", e);
      e.printStackTrace();
    }
  }

}
