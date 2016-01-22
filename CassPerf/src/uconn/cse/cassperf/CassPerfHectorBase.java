package uconn.cse.cassperf;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class CassPerfHectorBase {

	protected static Cluster cassPerfCluster;
	protected static Keyspace cassPerfKeyspace;
	protected static Keyspace computationalKeyspace;
	protected static Properties properties;

	protected static void initializeHectorLib() {
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

		cassPerfCluster = HFactory.getOrCreateCluster(
				properties.getProperty("cluster.name", "CassPerfCluster"),
				properties.getProperty("cluster.hosts", "127.0.0.1:9160"));
		// hcSystemCluster = HFactory.getOrCreateCluster("HealthCareCluster",
		// "10.12.2.209:9160");
		ConfigurableConsistencyLevel configConsistencyLevel = new ConfigurableConsistencyLevel();
		configConsistencyLevel
				.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
		configConsistencyLevel
				.setDefaultWriteConsistencyLevel(HConsistencyLevel.ANY);

		// healthCareKeyspace =
		// HFactory.createKeyspace(properties.getProperty("healthcare.keyspace",
		// "HealthCare"), hcSystemCluster, ccl);
		cassPerfKeyspace = HFactory.createKeyspace("CassExp", cassPerfCluster,
				configConsistencyLevel);

	}

	public static Cluster getCluster() {
		return cassPerfCluster;
	}

	public static Keyspace getDataKeyspace() {
		return cassPerfKeyspace;
	}
}
