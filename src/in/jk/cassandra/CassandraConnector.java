package in.jk.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

public class CassandraConnector {

	private Cluster cluster;
	private Session session;
	private String node ="127.0.0.1"; //local host Node IP
	private int port =9042; // Default port of Cassandra

	public Cluster connect(String node, int port) {
		
		SocketOptions options = new SocketOptions();
	    options.setConnectTimeoutMillis(300000);
	    options.setReadTimeoutMillis(3000000);
	    options.setTcpNoDelay(true);

		return this.cluster = Cluster.builder()
				                            .addContactPoint(node)
				                            .withPort(port)
				                            .withSocketOptions(options)
				                            .build();

	}

	public Session openSession() {
		
		String localhostNode = this.node;
		int port =this.port;

		this.cluster = connect(localhostNode, port);// provide cluster IP address and port no for getting cluster
														// instance . ;
		session = cluster.connect();
		return session;

	}

	public void close() {

		session.close();
	}

}
