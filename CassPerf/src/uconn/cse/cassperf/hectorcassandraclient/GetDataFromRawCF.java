package uconn.cse.cassperf.hectorcassandraclient;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Thrift API: http://wiki.apache.org/cassandra/API#get_slice
 *
 *
 */
//checked
public class GetDataFromRawCF extends GetRows {

    public GetDataFromRawCF() {
        init();
    }

    public QueryResult<ColumnSlice<Long, Double>> execute(String metric, Long uid, Long Start, Long End, boolean reserved, int count) {
        //Key, Column_Name, Column_Value
        SliceQuery<Long, Long, Double> sliceQuery =
                HFactory.createSliceQuery(keyspace, LongSerializer.get(), LongSerializer.get(), DoubleSerializer.get());

        sliceQuery.setColumnFamily(metric);
        sliceQuery.setKey(uid);

        // change 'reversed' to true to get the columns in reverse order
        sliceQuery.setRange(Start, End, reserved, count);

        QueryResult<ColumnSlice<Long, Double>> result = sliceQuery.execute();

        return result;
    }
}
