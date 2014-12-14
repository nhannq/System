package uconn.cse.cassperf;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import uconn.cse.cassperf.hectorcassandraclient.GetDataFromDataCF;
import uconn.cse.cassperf.utils.CheckData;
import uconn.cse.cassperf.utils.ReadData;

public class CassPerfRunner extends CassPerfHectorBase {

    GetDataFromDataCF getData = new GetDataFromDataCF();

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.out.println("Hi! Welcome to Cassandra peformance monitoring program:");
            System.out.println("0 to put benchmark data");
            System.out.println("1 logFileName to check the dropped message rate");
            return;
        }
        long startTime = System.currentTimeMillis();
        initializeHectorLib();
        int id = 0; //id to stores raw data of EOG data, reference of EOG and Images data
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("gendata.properties"));
            id = Integer.parseInt(properties.getProperty("id")); 
            System.out.println(id);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        int firstParameter = Integer.parseInt(args[0]);
        String secondParameter = args[1];

        if (firstParameter == 0) { //put data
            ReadData rD = new ReadData();
            int noOfReplica = 0;
            noOfReplica = Integer.parseInt(properties.getProperty("noOfReplica"));
            int noOfSamples = 0;
            noOfSamples = Integer.parseInt(properties.getProperty("noOfSamples"));
            int rate = 0;
            rate = Integer.parseInt(properties.getProperty("rate"));
            rD.generateDataforCassandra(id, noOfReplica, noOfSamples, rate);
        } else { //check data
            CheckData cD = new CheckData();
            int noOfReplica = 0;
            noOfReplica = Integer.parseInt(properties.getProperty("noOfReplica"));
            int noOfSamples = 0;
            noOfSamples = Integer.parseInt(properties.getProperty("noOfSamples"));
            int rate = 0;
            rate = Integer.parseInt(properties.getProperty("rate"));
            int delayTime = 0;
            delayTime = Integer.parseInt(properties.getProperty("delayTime"));
            String lcheckfile = properties.getProperty("lcheck");
            String logFileName = secondParameter; //the name of log file, it is used to get the start time of experiment to check the dropped rate per minute 
            cD.checkDatafromCassandra(id, noOfReplica, noOfSamples, lcheckfile, rate, delayTime, logFileName);
        }

        System.out.println("RT: " + (System.currentTimeMillis() - startTime));
    }
}
