package hu.sztaki.lpds.dataavenue.adaptors.cassandra;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import hu.sztaki.lpds.dataavenue.interfaces.CloseableSessionObject;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

public class CassandraSession implements CloseableSessionObject {
	
	private static final Logger log = LoggerFactory.getLogger(CassandraSession.class);
	
	// "a given session can only be set to one keyspace at a time, so one instance per keyspace is necessary!"
	private final Map<String, Session> sessions = new HashMap<String, Session> (); // map: host+keyspace -> session
	
	private String getSessionKey(final CassandraURI uri) { // host + keyspace, unique
		return uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "") + (uri.getKeyspace() != null ? CassandraURI.PATH_SEPARATOR + uri.getKeyspace() : "");
	}
	
	private String getHost(final CassandraURI uri) { // host + keyspace, unique
		return uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
	}
	
	/*CassandraSession(final URIBase uri, final String username, final String password) throws URIException {
		CassandraURI cURI = new CassandraURI(uri);
		add(cURI, username, password);
	}*/
	
	void put(final URIBase uri, final Credentials credentials) throws URIException, CredentialException {
		
		CassandraURI cURI = new CassandraURI(uri);

		Builder clusterBilder = cURI.getPort() == null ? 
				Cluster.builder().addContactPoints(cURI.getHost()) : // throws illegal argument exception
				Cluster.builder().addContactPointsWithPorts(Arrays.asList(new InetSocketAddress[] { new InetSocketAddress(cURI.getHost(), cURI.getPort())})); // throws illegal argument exception
		
		if (credentials != null) {
			if (CassandraAdaptor.USERPASS_AUTH.equals(credentials.getCredentialAttribute("Type")) && (credentials.getCredentialAttribute("UserID") == null || credentials.getCredentialAttribute("UserPass") == null)) 
				throw new CredentialException("No credentials provided");
			else 
				clusterBilder.withCredentials(credentials.getCredentialAttribute("UserID"), credentials.getCredentialAttribute("UserPass"));
		}
		Cluster cluster = clusterBilder.build(); 
				
		Metadata metadata = null;
		try { metadata = cluster.getMetadata(); } 
		catch (NoHostAvailableException x) { throw new URIException("Cannot connect to host: " + getHost(cURI) + "!", x); }
		catch (AuthenticationException  x) { throw new URIException("Authentication failed at connecting to host: " + getHost(cURI) + "!", x); }
		catch (IllegalStateException x) { throw new URIException("Cannot initialize cluster: " + getHost(cURI) + "!", x); }
		
		log.debug("Connected to cluster: {}", metadata.getClusterName());
		
		for (Host clusterNode: metadata.getAllHosts()) {
			log.debug("Datacenter: {}; Host: {}; Rack: {}", clusterNode.getDatacenter(), clusterNode.getAddress(), clusterNode.getRack());
		}
		// a given session can only be set to one keyspace at a time, so one instance per keyspace is necessary!
		Session session = cluster.connect();
		log.debug("Now connected");
		sessions.put(getSessionKey(cURI), session);
	}
	
	Session get(final URIBase uri) throws URIException {
		CassandraURI cURI = new CassandraURI(uri);
		return sessions.get(getSessionKey(cURI));
	}
	
	@Override
	public void close() {
		for (Session session: sessions.values()) {
			try { session.close(); log.debug("Session closed"); } 
			catch (Exception e) { log.warn("Cannot close session", e); }
			
			try { session.getCluster().close(); log.debug("Cluster closed"); } 
			catch (Exception e) { log.warn("Cannot close cluster", e); }
			//watch -n 5 -d "ps -eL <pid> | wc -l"
		}
	}
}