package uconn.cse.cassperf;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.datastax.driver.core.Session;
import com.netflix.astyanax.AstyanaxContext;
//import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
//import me.prettyprint.hector.api.Cluster;
//import me.prettyprint.hector.api.HConsistencyLevel;
//import me.prettyprint.hector.api.Keyspace;
//import me.prettyprint.hector.api.factory.HFactory;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassPerfAstyanaxBase {

	protected static AstyanaxContext<Keyspace> context;
	protected static Keyspace keyspace;
	// protected static Keyspace computationalKeyspace;
	protected static Properties properties;

	protected static void initializeDatastaxLib() {
		properties = new Properties();
		try {
			properties.load(new FileInputStream("gendata.properties"));
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		// To modify the default ConsistencyLevel of QUORUM, create a
		// me.prettyprint.hector.api.ConsistencyLevelPolicy and use the
		// overloaded form:
		// healthCareKeyspace = HFactory.createKeyspace("Tutorial",
		// hcSystemCluster, consistencyLevelPolicy);
		// see also
		// me.prettyprint.cassandra.model.ConfigurableConsistencyLevelPolicy[Test]
		// for details

		context = new AstyanaxContext.Builder()
				.forCluster(
						properties.getProperty("cluster.name",
								"CassPerfCluster"))
				.forKeyspace("CassExp")
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(9160).setSeeds(
										properties.getProperty("cluster.hosts",
												"127.0.0.1:9160")))
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl().setConnectionPoolType(
								ConnectionPoolType.TOKEN_AWARE)
								.setDiscoveryType(
										NodeDiscoveryType.RING_DESCRIBE)
								.setDefaultReadConsistencyLevel(ConsistencyLevel.CL_ONE)
								.setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_ALL))
				.buildKeyspace(ThriftFamilyFactory.getInstance());

		keyspace = context.getClient();

		context.start();
	}

	public static AstyanaxContext<Keyspace> getCluster() {
		return context;
	}

	public static Keyspace getDataKeyspace() {
		return keyspace;
	}

	// public static void close() {
	// cassPerfCluster.close();
	// }
}
