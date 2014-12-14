/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.hectorcassandraclient;

import me.prettyprint.hector.api.Keyspace;
import uconn.cse.cassperf.CassPerfHectorBase;

/**
 *
 * @author nhannguyen
 */
//checked
public class GetRows {

    protected static Keyspace keyspace;

    protected static void init() {
        keyspace = CassPerfHectorBase.getDataKeyspace();
    }
}
