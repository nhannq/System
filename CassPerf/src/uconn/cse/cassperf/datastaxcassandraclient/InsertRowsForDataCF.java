/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.datastaxcassandraclient;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;

import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * 
 * @author nhannguyen
 */
public class InsertRowsForDataCF extends InsertRows {

	public InsertRowsForDataCF() {
		init();
	}

	public QueryResult<?> executeOneColumn(int rowName, int key) {

		// put nbKeys keys to Cassandra
		session.execute("insert into Data (rowName, columnName, v) VALUES (" + rowName + "," + key + ", 1);");
		return null;
	}

	// put data through a list
	// public QueryResult<?> execute2(int RowName, int tsID, int rate) {
	public long executeMultiColumns(int rowName, int tsID, int rate) {
		// Key
		PreparedStatement ps = session.prepare("insert into Data (rowName, columnName, v) VALUES (?,?,?)");
		BatchStatement bs = new BatchStatement();
//		String cqlCommand = "BEGIN BATCH ";
		// CF name, CF value
		for (int key = tsID; key < tsID + rate; key++) {
			bs.add(ps.bind(rowName, key, 1.0));
		}
		
		session.execute(bs);
//		cqlCommand += "APPLY BATCH;";
//		session.execute(cqlCommand);
		// mR.g

		// mutator.addInsertion(RowName, CFName, HFactory.createColumn(Key,
		// Value, LongSerializer.get(), LongSerializer.get()));
		// mutator.execute();
		return 1;
	}
}
