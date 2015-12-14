package hadooplogparser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

class MethodInformation {
  public int nbMethodCalls;
  public int runTime;
  public double avgNbCallsPerSec;

  public MethodInformation(int nbMethodCalls, int runTime, double avgNbCallsPerSec) {
    this.nbMethodCalls = nbMethodCalls;
    this.runTime = runTime;
    this.avgNbCallsPerSec = avgNbCallsPerSec;
  }
}


class MethodCall {
  String testCase;
  ArrayList<MethodInformation> methodInfo = new ArrayList<MethodInformation>(4);

  public MethodCall(String testCase) {
    for (int i = 0; i < 4; i++)
      methodInfo.add(null);
    this.testCase = testCase;
  }

  public void addMethodInfo(int index, MethodInformation m) {
    // System.out.println("index " + index);
    methodInfo.add(index, m);
  }
}


class MethodRunTime {
  String testCase;
  ArrayList<MethodInformation> methodInfo = new ArrayList<MethodInformation>(4);

  public MethodRunTime(String testCase) {
    for (int i = 0; i < 4; i++)
      methodInfo.add(null);
    this.testCase = testCase;
  }

  public void addMethodInfo(int index, MethodInformation m) {
    methodInfo.add(index, m);
  }
}


class MethodTS {
  public ArrayList<ArrayList<String>> timeStampList = new ArrayList<ArrayList<String>>();
  public int maxTimeStampElmenetNumbers;

  public MethodTS(ArrayList<ArrayList<String>> timeStampList) {
    this.timeStampList = timeStampList;
  }

  public void addMaxTimeStampElmenetNumbers(int maxTimeStampElmenetNumbers) {
    this.maxTimeStampElmenetNumbers = maxTimeStampElmenetNumbers;
  }
}


class MethodTimeStamp {
  String testCase;
  ArrayList<MethodTS> methodTimeStamps = new ArrayList<MethodTS>(4);

  public MethodTimeStamp(String testCase) {
    for (int i = 0; i < 4; i++)
      methodTimeStamps.add(null);
    this.testCase = testCase;
  }

  public void addMethodTimeStamp(int index, MethodTS m) {
    methodTimeStamps.add(index, m);
  }
}


public class Parser {

  public static void readFile(String fileName, Collection<String> data) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(fileName));
      try {
        String line = br.readLine();
        while (line != null) {
          data.add(line.trim());
          // System.out.println("LINE " + line);
          line = br.readLine();
        }
      } finally {
        br.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    TreeMap<String, String> testInfoMap = new TreeMap<String, String>();
    TreeSet<String> runNoSet = new TreeSet<String>();
    if (args[0].equals("0")) {
      // java -jar HadoopLogParser.jar 0 setting1 hadooplogdump
      String settingFile = args[1];
      String hadoopLogDumpFolder = args[2];
      String settingID = args[3];
      try {
        BufferedReader br = new BufferedReader(new FileReader(settingFile + ".out"));
        int countLine = 0;
        try {
          String line = br.readLine();
          String testNo = "";
          while (line != null) {
            if (line.contains("RUNNO")) {
              testNo = line.substring(5, line.length());
              runNoSet.add(testNo);
            }
            if (line.contains("INFO mapreduce.Job: Running job:")) {
              testInfoMap.put(line.split(":")[4].trim(), testNo);
            }
            line = br.readLine();
          }
          // System.out.println(testInfo.toString());
        } finally {
          br.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      // for (String key : testInfoMap.keySet()) {
      // System.out.println(key + "\t" + testInfoMap.get(key));
      // }

      HashMap<String, Integer> runNoToExperimentID = new HashMap<String, Integer>();
      FileWriter fw = new FileWriter("runCount.sh");
      FileWriter fwRunTime = new FileWriter("runRunTime.sh");
      FileWriter fwTimeStamp = new FileWriter("runTimeStamp.sh");
      FileWriter fwLogCleaner = new FileWriter("runLogCleaner.sh");
      for (String runNo : runNoSet) {
//         System.out.println("runNo " + runNo);
        try {
          BufferedReader br =
              new BufferedReader(new FileReader(hadoopLogDumpFolder + "/n-job-trace-" + settingFile
                  + "-log" + runNo + "-" + settingID+  ".json"));
          try {
            String line = br.readLine();
            StringBuilder testInfo = new StringBuilder();
            boolean firstFinishTimeLine = false;
            boolean needToRecord = false;
            String jobID = "";
            while (line != null) {

              if (line.contains("\"jobID\" :")) {
                jobID = line.split(":")[1].replace("\"", "").replace(",", "").trim();
                if (testInfoMap.containsKey(jobID)) {
                  // System.out.println(jobID);
                  if (testInfoMap.get(jobID).equals(runNo)) {
                    if (!testInfo.toString().isEmpty()) {
                      long runtime =
                          (Long.parseLong(testInfo.toString().split("\t")[3]) - Long
                              .parseLong(testInfo.toString().split("\t")[2])) / 1000;
                      // System.out.println(testInfo.toString() + "\t" + runtime); // print to
                      // log.out
                      System.out.println(testInfo.toString().split("\t")[0]
                          + "\t"
                          + testInfo.toString().split("\t")[1].substring(
                              testInfo.toString().split("\t")[1].length() - 2, testInfo.toString()
                                  .split("\t")[1].length()) + "\t" + runtime);
                    }
                    testInfo = new StringBuilder();

                    // System.out.println(line);
                    testInfo.append(testInfoMap.get(jobID));
                    testInfo.append("\t").append(jobID);
                    fw.write("echo " + runNo + "\n");
                    fw.write("/home/nqn12001/hadooplog/cmd/count" + runNo
                        + ".sh $1/logs/userlogs/application_" + jobID.split("_")[1] + "_"
                        + jobID.split("_")[2] + " > " + "tmpCount/rs" + runNo + "_"
                        + jobID.subSequence(jobID.length() - 2, jobID.length()) + "\n");
                    fwRunTime.write("echo " + runNo + "\n");
                    fwRunTime.write("/home/nqn12001/hadooplog/cmd/runtime" + runNo
                        + ".sh $1/logs/userlogs/application_" + jobID.split("_")[1] + "_"
                        + jobID.split("_")[2] + " > " + "tmpRunTime/rs" + runNo + "_"
                        + jobID.subSequence(jobID.length() - 2, jobID.length()) + "\n");
                    fwTimeStamp.write("echo " + runNo + "\n");
                    fwTimeStamp.write("/home/nqn12001/hadooplog/cmd/count" + runNo
                        + "-1.sh $1/logs/userlogs/application_" + jobID.split("_")[1] + "_"
                        + jobID.split("_")[2] + " > " + "tmpTimeStamp/rs" + runNo + "_"
                        + jobID.subSequence(jobID.length() - 2, jobID.length()) + "\n");
                    fwLogCleaner.write("mv $1/application_" + jobID.split("_")[1] + "_"
                        + jobID.split("_")[2] + " $2\n");
                    needToRecord = true;
                  } else
                    needToRecord = false;
                }
                firstFinishTimeLine = false;
              }
              if (needToRecord && line.contains("submitTime")) {
                testInfo.append("\t").append(line.split(":")[1].replace(",", "").trim());
              }
              if (needToRecord && line.contains("finishTime") && !firstFinishTimeLine) {
                testInfo.append("\t").append(line.split(":")[1].replace(",", "").trim());
                firstFinishTimeLine = true;
              }
              line = br.readLine();
            }
            long runtime =
                (Long.parseLong(testInfo.toString().split("\t")[3]) - Long.parseLong(testInfo
                    .toString().split("\t")[2])) / 1000;
            // System.out.println(testInfo.toString() + "\t" + runtime); //print to log.out
            System.out.println(testInfo.toString().split("\t")[0]
                + "\t"
                + testInfo.toString().split("\t")[1].substring(testInfo.toString().split("\t")[1]
                    .length() - 2, testInfo.toString().split("\t")[1].length()) + "\t" + runtime);
          } finally {
            br.close();
          }
        } catch (Exception e) {
          // e.printStackTrace();
        }
      }
      fw.close();
      fwRunTime.close();
      fwTimeStamp.close();
      fwLogCleaner.close();
    } else if (args[0].equals("1")) {
      // java -jar HadoopLogParser.jar 1 log.out getFinalRS/tmpCount getFinalRS/tmpRunTime 3
      FileWriter methodCallResultFile = new FileWriter("MethodCall.csv");
      FileWriter runtimeFile = new FileWriter("MethodRunTime.csv");
      FileWriter timeStampFile = new FileWriter("MethodTimeStamp.csv");
      String runtimeFileName = args[1];
      String tmpCountFolder = args[2];
      String tmpRunTimeFolder = args[3];
      String tmpTimeStampFolder = args[4];
      ArrayList<String> testAndRuntimeList = new ArrayList<String>();
      readFile(runtimeFileName, testAndRuntimeList);
      ArrayList<String> methodNames = new ArrayList<String>();
      int nbNodes = Integer.parseInt(args[5]); // number of master & slave nodes
      System.out.println("nbNodes " + nbNodes);
      ArrayList<ArrayList<String>> nbMethodCalls = new ArrayList<ArrayList<String>>(nbNodes);
      LinkedHashMap<String, MethodCall> methodCallMap = new LinkedHashMap<String, MethodCall>();
      LinkedHashMap<String, MethodRunTime> methodRunTimeMap =
          new LinkedHashMap<String, MethodRunTime>();
      LinkedHashMap<String, Integer> methodRTMap = new LinkedHashMap<String, Integer>();
      LinkedHashMap<String, Integer> methodRTCallMap = new LinkedHashMap<String, Integer>();
      LinkedHashMap<String, MethodTimeStamp> methodTimeStampMap =
          new LinkedHashMap<String, MethodTimeStamp>();
      ArrayList<ArrayList<String>> timeStampList = new ArrayList<ArrayList<String>>();
      int maxTimeStampElmenetNumbers = -1;

      for (String r : testAndRuntimeList) { // r: testID \t case \t runtime

        // GET METHOD NAME
        System.out.println("/home/nqn12001/hadooplog/cmd/printMethodName" + r.split("\t")[0]
            + ".sh");
        // String fName = "/home/nn/Research/Projects/PerfModel/Hadoop/cmd/printMethodName";
        String fName = "/home/nqn12001/hadooplog/cmd/printMethodName";
        readFile(fName + r.split("\t")[0] + ".sh", methodNames);

        // GET NUMBER OF METHOD CALLS
        for (int i = 0; i < nbNodes; i++) {
          ArrayList<String> tmpList = new ArrayList<String>();

          readFile(tmpCountFolder + (i + 1) + "/rs" + r.split("\t")[0] + "_" + r.split("\t")[1],
              tmpList);
          // System.out.println(tmpCountFolder + (i + 1) + "/rs" + r.split("\t")[0] + "_"
          // + r.split("\t")[1] + " size " + tmpList.size());
          nbMethodCalls.add(tmpList);
        }

        System.out.println("nbMethodCall Size " + nbMethodCalls.size());
        System.out.println("methodNames.size() " + methodNames.size());
        ArrayList<Integer> totalNbMethodCalls = new ArrayList<Integer>();

        // ADD THE NUMBER OF METHOD CALLS FROM ALL CLIENTS
        for (int i = 0; i < methodNames.size(); i++) {
          int tmpNbCall = 0;
          for (int j = 0; j < nbNodes; j++)
            if (i < nbMethodCalls.get(j).size())
              tmpNbCall += Integer.parseInt(nbMethodCalls.get(j).get(i));

          totalNbMethodCalls.add(tmpNbCall);
        }

        // MAP NUMBER OF METHOD CALLS TO METHOD NAMES, DO SOME BASIC ANALYSIS
        for (int i = 0; i < methodNames.size(); i++) {
          if (totalNbMethodCalls.get(i) > 0) {
            MethodCall mCall;
            if (methodCallMap.containsKey(methodNames.get(i))) {
              mCall = methodCallMap.get(methodNames.get(i));
            } else
              mCall = new MethodCall(r.split("\t")[0]);
            mCall.addMethodInfo(Integer.parseInt(r.split("\t")[1]), new MethodInformation(
                totalNbMethodCalls.get(i), Integer.parseInt(r.split("\t")[2]), 1.0
                    * totalNbMethodCalls.get(i) / Integer.parseInt(r.split("\t")[2])));
            methodCallMap.put(methodNames.get(i), mCall);
          }
        }

        // GET RUN TIME
        methodRTMap.clear();
        methodRTCallMap.clear();

//        for (int i = 0; i < nbNodes; i++) {
//          ArrayList<String> tmpList = new ArrayList<String>();
//
//          readFile(tmpRunTimeFolder + (i + 1) + "/rs" + r.split("\t")[0] + "_" + r.split("\t")[1],
//              tmpList);
//          // System.out.println(tmpRunTimeFolder + (i + 1) + "/rs" + r.split("\t")[0] + "_"
//          // + r.split("\t")[1] + " size " + tmpList.size());
//          for (String s : tmpList) {
//            // System.out.println(s);
//            String methodName = s.substring(s.indexOf("Runtime") + 8, s.length()).split("\t")[0];
//            int methodRuntime =
//                Integer.parseInt(s.substring(s.indexOf("Runtime") + 8, s.length()).split("\t")[2]
//                    .trim());
//            int totalRT = 0;
//            if (methodRTMap.containsKey(methodName)) {
//              totalRT = methodRTMap.get(methodName);
//            }
//            totalRT += methodRuntime;
//            methodRTMap.put(methodName, totalRT);
//            int nbCalls = 0;
//            if (methodRTCallMap.containsKey(methodName)) {
//              nbCalls = methodRTCallMap.get(methodName);
//            }
//            nbCalls += 1;
//            methodRTCallMap.put(methodName, nbCalls);
//          }
//
//          System.out.println("methodRTMap " + methodRTMap.size());
//        }
//
//        for (String s : methodRTMap.keySet()) { // method name
//          MethodRunTime mRT;
//          if (methodRunTimeMap.containsKey(s)) {
//            mRT = methodRunTimeMap.get(s);
//          } else
//            mRT = new MethodRunTime(r.split("\t")[0]);
//          mRT.addMethodInfo(Integer.parseInt(r.split("\t")[1]), new MethodInformation(
//              methodRTCallMap.get(s), methodRTMap.get(s), methodRTMap.get(s)));
//          methodRunTimeMap.put(s, mRT);
//        }

        nbMethodCalls.clear();
        methodNames.clear();


        String methodName = null;
        maxTimeStampElmenetNumbers = -1;
        for (int i = 0; i < nbNodes; i++) {
          ArrayList<String> tmpList = new ArrayList<String>();

          readFile(
              tmpTimeStampFolder + (i + 1) + "/rs" + r.split("\t")[0] + "_" + r.split("\t")[1],
              tmpList);
          if (tmpList.size() > maxTimeStampElmenetNumbers) {
            maxTimeStampElmenetNumbers = tmpList.size();
          }
          // System.out.println(tmpRunTimeFolder + (i + 1) + "/rs" + r.split("\t")[0] + "_"
          // + r.split("\t")[1] + " size " + tmpList.size());

//          String tsStart = null;
          String tsEnd = null;
          ArrayList<String> timeStampInANode = new ArrayList<String>();
          for (String s : tmpList) {
            // System.out.println(s);

            if (s.contains("StartMethod")) {
              if (methodName == null)
                methodName = s.substring(s.indexOf("StartMethod") + 12, s.length()).split("\t")[0];
//              tsStart = s.substring(s.indexOf("StartMethod") + 12, s.length()).split("\t")[1];
            } else if (s.contains("RunTime")) {
              tsEnd = s.substring(s.indexOf("Runtime") + 8, s.length()).split("\t")[1].trim();
//              int methodRuntime =
//                  Integer.parseInt(s.substring(s.indexOf("Runtime") + 8, s.length()).split("\t")[2]
//                      .trim());
              timeStampInANode.add(tsEnd);
            }
          }
          timeStampList.add(timeStampInANode);
          System.out.println("methodRTMap " + methodRTMap.size());
        }
        // Integer.parseInt(r.split("\t")[1])
        MethodTS mTS = new MethodTS(timeStampList);
        mTS.addMaxTimeStampElmenetNumbers(maxTimeStampElmenetNumbers);
        if (methodName != null) {
          MethodTimeStamp mTimeStamp;
          if (methodTimeStampMap.containsKey(methodName)) {
            mTimeStamp = methodTimeStampMap.get(methodName);
          } else
            mTimeStamp = new MethodTimeStamp(r.split("\t")[0] + "_" + r.split("\t")[1]);
          mTimeStamp.addMethodTimeStamp(Integer.parseInt(r.split("\t")[1]), mTS);
          methodTimeStampMap.put(methodName, mTimeStamp);
        }
      }

      for (String k : methodCallMap.keySet()) {
        StringBuilder rs = new StringBuilder();
        for (int i = 1; i < 5; i++) {
          MethodInformation m = methodCallMap.get(k).methodInfo.get(i);
          if (m == null)
            rs.append("0\t0\t0\t");
          else
            rs.append(m.nbMethodCalls).append("\t").append(m.runTime).append("\t")
                .append(String.format("%f", m.avgNbCallsPerSec)).append("\t");
        }
        methodCallResultFile.write(methodCallMap.get(k).testCase + "\t" + k + "\t" + rs.toString()
            + "\n");
      }

      for (String k : methodRunTimeMap.keySet()) { // method name
        System.out.println("size " + methodRunTimeMap.get(k).methodInfo.size());
        StringBuilder rs = new StringBuilder();
        for (int i = 1; i < 5; i++) {
          MethodInformation m = methodRunTimeMap.get(k).methodInfo.get(i);
          if (m == null)
            rs.append("0\t0\t0\t");
          else
            rs.append(m.nbMethodCalls).append("\t").append(m.runTime).append("\t")
                .append(String.format("%f", 1.0 * m.runTime / m.nbMethodCalls)).append("\t");
        }
        runtimeFile
            .write(methodRunTimeMap.get(k).testCase + "\t" + k + "\t" + rs.toString() + "\n");
      }

      for (String k : methodTimeStampMap.keySet()) {
        MethodTimeStamp mTimeStamp = methodTimeStampMap.get(k);
        StringBuilder rs = new StringBuilder();
        for (int i = 1; i < 4; i++) {
          MethodTS m = mTimeStamp.methodTimeStamps.get(i);
          if (m == null) {
            rs.append("0\t0\t0\t");
          } else {
            for (int j = 0; j < m.maxTimeStampElmenetNumbers; j++) {
              if (j < m.timeStampList.get(0).size())
                rs.append(m.timeStampList.get(0).get(j));
              else
                rs.append("0\t0\t0\t");
              if (j < m.timeStampList.get(1).size())
                rs.append(m.timeStampList.get(1).get(j));
              else
                rs.append("0\t0\t0\t");
              if (j < m.timeStampList.get(2).size())
                rs.append(m.timeStampList.get(2).get(j));
              else
                rs.append("0\t0\t0\t");
              rs.append("\n");
            }
          }
        }
        timeStampFile.write(methodTimeStampMap.get(k).testCase + "\t" + k + "\n");
        timeStampFile.write(rs.toString());
      }
      runtimeFile.close();
      methodCallResultFile.close();
      timeStampFile.close();
    }
  }
}
