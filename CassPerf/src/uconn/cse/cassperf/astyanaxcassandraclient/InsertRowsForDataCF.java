/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.astyanaxcassandraclient;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author nhannguyen
 */
public class InsertRowsForDataCF extends InsertRows {
	private static Logger logger = LoggerFactory.getLogger(InsertRowsForDataCF.class); 
	public InsertRowsForDataCF() {
		init();
	}

	public void executeOneColumn(int RowName, int Key)
			throws ConnectionException {
		// Key
		keyspace.prepareColumnMutation(CF_STANDARD1, RowName, Key)
				.putValue(1.0, null).execute();
		return;
	}

	// put data through a list
	// public QueryResult<?> execute2(int RowName, int tsID, int rate) {
	public void executeMultiColumns(int RowName, int tsID, int rate) {
		MutationBatch m = keyspace.prepareMutationBatch();

		// CF name, CF value
		for (int i = tsID; i < tsID + rate; i++) {
			m.withRow(CF_STANDARD1, RowName).putColumn(i, 1.0);
		}
		try {
			// OperationResult<Void> result =
			m.execute();
		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			logger.debug("failed to write data to server",e);
			e.printStackTrace();
		}
		return;
	}
}
