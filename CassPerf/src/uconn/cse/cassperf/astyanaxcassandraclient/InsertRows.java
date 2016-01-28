/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.astyanaxcassandraclient;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

import uconn.cse.cassperf.CassPerfAstyanaxBase;

/**
 *
 * @author nhannguyen
 */
//checked
public class InsertRows {

    protected static Keyspace keyspace;
    protected static ColumnFamily<Integer, Integer> CF_STANDARD1;

    protected static void init() {
        keyspace = CassPerfAstyanaxBase.getDataKeyspace();
        CF_STANDARD1 = new ColumnFamily<Integer, Integer>("Data", IntegerSerializer.get(), IntegerSerializer.get(), DoubleSerializer.get());
    }
}
