/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.hectorcassandraclient;

import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import static uconn.cse.cassperf.hectorcassandraclient.GetRows.init;
import static uconn.cse.cassperf.hectorcassandraclient.GetRows.keyspace;

/*
 * Get raw data to perform motif mining 
 * @author nhannguyen
 */
public class GetDataFromDataCF extends GetRows {

    SliceQuery<Integer, Integer, Double> sliceQuery;

    public GetDataFromDataCF() {
        init();
    }

    /*
     * Start: start pos of query -> id of SQ
     * End: end pos of query -> id + length - 1
     */
    public QueryResult<ColumnSlice<Integer, Double>> execute(String metric, int uid, int Start, int End, boolean reserved, int count) {
        //Key, Column_Name, Column_Value
        sliceQuery =
                HFactory.createSliceQuery(keyspace, IntegerSerializer.get(), IntegerSerializer.get(), DoubleSerializer.get());

        sliceQuery.setColumnFamily(metric);
        sliceQuery.setKey(uid);

        // change 'reversed' to true to get the columns in reverse order
        sliceQuery.setRange(Start, End, reserved, count);

        return sliceQuery.execute();
    }

    public QueryResult<ColumnSlice<Integer, Double>> execute2(int uid, int count) { //brief version
        //Key, Column_Name, Column_Value
        sliceQuery =
                HFactory.createSliceQuery(keyspace, IntegerSerializer.get(), IntegerSerializer.get(), DoubleSerializer.get());

        sliceQuery.setColumnFamily("Data");
        sliceQuery.setKey(uid);

        // change 'reversed' to true to get the columns in reverse order
        sliceQuery.setRange(0, count - 1, false, count);

        return sliceQuery.execute();
    }
}
