/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.datastaxcassandraclient;

import com.datastax.driver.core.ResultSet;


/*
 * Get raw data to perform motif mining 
 * @author nhannguyen
 */
public class GetDataFromDataCF extends GetRows {


    public GetDataFromDataCF() {
        init();
    }

    /*
     * Start: start pos of query -> id of SQ
     * End: end pos of query -> id + length - 1
     */
    public ResultSet execute(String metric, int uid, int Start, int End, boolean reserved, int count) {
        //Key, Column_Name, Column_Value
    	String cqlCommand = "select * from Data where rowName=" + uid + " and columnName >= " + Start + " and columnName <= " + End + " limit 50000;";
//    	System.out.println(cqlCommand);
    	return session.execute(cqlCommand);
//        sliceQuery =
//                HFactory.createSliceQuery(keyspace, IntegerSerializer.get(), IntegerSerializer.get(), DoubleSerializer.get());
//
//        sliceQuery.setColumnFamily(metric);
//        sliceQuery.setKey(uid);
//
//        // change 'reversed' to true to get the columns in reverse order
//        sliceQuery.setRange(Start, End, reserved, count);
//
//        return sliceQuery.execute();
    }

//    public QueryResult<ColumnSlice<Integer, Double>> execute2(int uid, int count) { //brief version
//        //Key, Column_Name, Column_Value
//        sliceQuery =
//                HFactory.createSliceQuery(keyspace, IntegerSerializer.get(), IntegerSerializer.get(), DoubleSerializer.get());
//
//        sliceQuery.setColumnFamily("Data");
//        sliceQuery.setKey(uid);
//
//        // change 'reversed' to true to get the columns in reverse order
//        sliceQuery.setRange(0, count - 1, false, count);
//
//        return sliceQuery.execute();
//    }
}
