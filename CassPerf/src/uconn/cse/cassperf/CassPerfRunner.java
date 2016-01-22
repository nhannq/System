package uconn.cse.cassperf;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import uconn.cse.cassperf.utils.CheckData;
import uconn.cse.cassperf.utils.PutData;

public class CassPerfRunner {
  
  static void printInfo(int noOfReplica, int minute, int rate) {
    System.out.println("noOfReplica " + noOfReplica);
    System.out.println("minute " + minute);
    System.out.println("rate " + rate);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 2) {
      System.out.println("Hi! Welcome to Cassandra peformance monitoring program:");
      System.out.println("0 0 to put benchmark data");
      System.out.println("1 logFileName to check the dropped message rate");
      return;
    }
    int id = 0; // id to stores raw data of EOG data, reference of EOG and
    // Images data
    Properties properties = new Properties();
    try {
      properties.load(new FileInputStream("gendata.properties"));
      id = Integer.parseInt(properties.getProperty("id"));
      System.out.println(id);
    } catch (IOException ex) {
      ex.printStackTrace();
    }
    int driver = Integer.parseInt(properties.getProperty("driver", "0"));
    long startTime = System.currentTimeMillis();
    if (driver == 0) { // use Hector library
      System.out.println("Using the Hector library");
      CassPerfHectorBase.initializeHectorLib();

      int firstParameter = Integer.parseInt(args[0]);
      String logFileName = args[1];

      if (firstParameter == 0) { // put data
        PutData rD = new PutData();
        int noOfReplica = 0;
        noOfReplica = Integer.parseInt(properties.getProperty("noOfReplica"));
        int minute = 0;
        minute = Integer.parseInt(properties.getProperty("minute"));
        int rate = 0;
        rate = Integer.parseInt(properties.getProperty("rate"));
        printInfo(noOfReplica, minute, rate);
        rD.generateDataforCassandraHector(id, noOfReplica, minute, rate);
      } else { // check data
        CheckData cD = new CheckData();
        int noOfReplica = 0;
        noOfReplica = Integer.parseInt(properties.getProperty("noOfReplica"));
        int minute = 0;
        minute = Integer.parseInt(properties.getProperty("minute"));
        int rate = 0;
        rate = Integer.parseInt(properties.getProperty("rate"));
        int delayTime = 0;
        delayTime = Integer.parseInt(properties.getProperty("delayTime"));
        String lcheckfile = properties.getProperty("lcheck");
        System.out.println("minute " + minute);
        System.out.println("rate " + rate);
        System.out.println("delayTime " + delayTime);
        System.out.println("lcheck " + lcheckfile);
        cD.checkDatafromCassandraHectorPerSecond(id, noOfReplica, minute, lcheckfile, rate, delayTime,
            logFileName);
      }
    } else if (driver == 1) { // use Datastax library
      System.out.println("Using Datastax Java Driver");
      CassPerfDatastaxBase.initializeDatastaxLib();

      int firstParameter = Integer.parseInt(args[0]);
      String logFileName = args[1];

      if (firstParameter == 0) { // put data
        PutData rD = new PutData();
        int noOfReplica = 0;
        noOfReplica = Integer.parseInt(properties.getProperty("noOfReplica"));
        int minute = 0;
        minute = Integer.parseInt(properties.getProperty("minute"));
        int rate = 0;
        rate = Integer.parseInt(properties.getProperty("rate"));
        rD.generateDataforCassandraDatastax(id, noOfReplica, minute, rate);
      } else { // check data
        CheckData cD = new CheckData();
        int noOfReplica = 0;
        noOfReplica = Integer.parseInt(properties.getProperty("noOfReplica"));
        int minute = 0;
        minute = Integer.parseInt(properties.getProperty("minute"));
        int rate = 0;
        rate = Integer.parseInt(properties.getProperty("rate"));
        int delayTime = 0;
        delayTime = Integer.parseInt(properties.getProperty("delayTime"));
        String lcheckfile = properties.getProperty("lcheck");
        cD.checkDatafromCassandraDatastax(id, noOfReplica, minute, lcheckfile, rate,
            delayTime, logFileName);
      }
      CassPerfDatastaxBase.close();
    } else {
      System.out.println("Using Astyanax library");
      CassPerfAstyanaxBase.initializeDatastaxLib();

      int firstParameter = Integer.parseInt(args[0]);
      String logFileName = args[1];

      if (firstParameter == 0) { // put data
        PutData rD = new PutData();
        int noOfReplica = 0;
        noOfReplica = Integer.parseInt(properties.getProperty("noOfReplica"));
        int minute = 0;
        minute = Integer.parseInt(properties.getProperty("minute"));
        int rate = 0;
        rate = Integer.parseInt(properties.getProperty("rate"));
        rD.generateDataforCassandraAstyanax(id, noOfReplica, minute, rate);
      } else if (firstParameter == -1) {
        PutData rD = new PutData();
        int noOfReplica = 0;
        noOfReplica = Integer.parseInt(properties.getProperty("noOfReplica"));
        int minute = 0;
        minute = Integer.parseInt(properties.getProperty("minute"));
        int rate = 0;
        rate = Integer.parseInt(properties.getProperty("rate"));
        rD.generateDataforCassandraAstyanaxOneClient(id, noOfReplica, minute, rate);
      } else { // check data
        CheckData cD = new CheckData();
        int noOfReplica = 0;
        noOfReplica = Integer.parseInt(properties.getProperty("noOfReplica"));
        int minute = 0;
        minute = Integer.parseInt(properties.getProperty("minute"));
        int rate = 0;
        rate = Integer.parseInt(properties.getProperty("rate"));
        int delayTime = 0;
        delayTime = Integer.parseInt(properties.getProperty("delayTime"));
        String lcheckfile = properties.getProperty("lcheck");
        cD.checkDatafromCassandraAstyanax(id, noOfReplica, minute, lcheckfile, rate,
            delayTime, logFileName);
      }
      // CassPerfDatastaxBase.close();
    }

    System.out.println("RT: " + (System.currentTimeMillis() - startTime));
  }
}
