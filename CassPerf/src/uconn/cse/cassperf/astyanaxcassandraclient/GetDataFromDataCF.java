/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.astyanaxcassandraclient;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.util.RangeBuilder;


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
    public ColumnList<Integer> execute(String metric, int uid, int start, int end, boolean reserved, int count) {
    	ColumnList<Integer> result = null;
    	try {
			result = keyspace.prepareQuery(CF_STANDARD1)
			   .getKey(uid)
			   .withColumnRange(start, end, reserved, count)
			   .execute().getResult();
		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        return result;
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
