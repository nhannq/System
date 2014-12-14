/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.hectorcassandraclient;

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

	public QueryResult<?> executeOneColumn(int RowName, int Key) {
		// Key
		Mutator<Integer> mutator = HFactory.createMutator(keyspace,
				IntegerSerializer.get());

		// CF name, CF value
		HColumn<Integer, Double> hc = HFactory.createColumn(Key, 1.0,
				IntegerSerializer.get(), DoubleSerializer.get());
		mutator.insert(RowName, "Data", hc);

		// mutator.addInsertion(RowName, CFName, HFactory.createColumn(Key,
		// Value, LongSerializer.get(), LongSerializer.get()));
		// mutator.execute();
		return null;
	}

	// put data through a list
//	public QueryResult<?> execute2(int RowName, int tsID, int rate) {
		public long executeMultiColumns(int RowName, int tsID, int rate) {
		// Key
		Mutator<Integer> mutator = HFactory.createMutator(keyspace,
				IntegerSerializer.get());

		// CF name, CF value
		for (int i = tsID; i < tsID + rate; i++) {
			HColumn<Integer, Double> hc = HFactory.createColumn(i, 1.0,	IntegerSerializer.get(), DoubleSerializer.get());
			mutator.addInsertion(RowName, "Data", hc);
		}
		MutationResult mR = mutator.execute();
//		mR.g

		// mutator.addInsertion(RowName, CFName, HFactory.createColumn(Key,
		// Value, LongSerializer.get(), LongSerializer.get()));
		// mutator.execute();
		return mR.getExecutionTimeMicro();
	}
}
