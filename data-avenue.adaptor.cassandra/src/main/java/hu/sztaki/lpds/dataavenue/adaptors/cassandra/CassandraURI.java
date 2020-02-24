package hu.sztaki.lpds.dataavenue.adaptors.cassandra;

import java.net.URI;
import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

public class CassandraURI implements URIBase {
	private static final Logger log = LoggerFactory.getLogger(CassandraURI.class);
	
	static final String PATH_SEPARATOR = "/";
	private final URI cassandraURI; // java.net.uri
	
	CassandraURI(final URIBase uri) throws URIException {
		if (uri == null) throw new IllegalArgumentException("null argument");
		
//		log.debug(uri.getURI());
		
		try { cassandraURI = new URI(uri.getURI()).normalize(); } 
		catch (URISyntaxException e) { throw new URIException("Invalid URI", e); }
		
		if (cassandraURI.getScheme() == null) throw new URIException("Missing protocol name"); // required
		if (cassandraURI.getHost() == null) throw new URIException("Missing host name"); // required
		
		if (!CassandraAdaptor.CASSANDRA_PROTOCOL.equals(cassandraURI.getScheme())) throw new URIException("Invalid protocol (" + CassandraAdaptor.CASSANDRA_PROTOCOL + " expected)" ); // verify protocol
		// the path can contain / | /keyspace | /keyspace/table but not /keyspace/table/somethingelse
		if (getTable() != null && getTable().contains(PATH_SEPARATOR)) throw new URIException("Malformed URI (table name must not contain '" + PATH_SEPARATOR + "')");
	}

	CassandraURI(final String uri) throws URIException {
		if (uri == null) throw new IllegalArgumentException("null argument");
		
		try {
			log.debug("Creating cassandra URI: {}", uri);
			cassandraURI = new URI(uri).normalize(); 
		} catch (URISyntaxException e) { throw new URIException("Invalid URI: '" + uri + "'", e); }
		
		if (cassandraURI.getScheme() == null) throw new URIException("Missing protocol name"); // required
		if (cassandraURI.getHost() == null) throw new URIException("Missing host name"); // required
		
		if (!CassandraAdaptor.CASSANDRA_PROTOCOL.equals(cassandraURI.getScheme())) throw new URIException("Invalid protocol"); // verify protocol
	}

	@Override public URIType getType() {
		if (cassandraURI.getPath() == null || cassandraURI.getPath().length() == 0 || PATH_SEPARATOR.equals(cassandraURI.getPath())) return URIType.URL;
		else if (cassandraURI.getPath().endsWith(PATH_SEPARATOR)) return URIType.DIRECTORY;
		else return URIType.FILE;
	}
	
	@Override public String getProtocol() {	
		return cassandraURI.getScheme();	
	}
	
	@Override public String getHost() { 
		return cassandraURI.getHost(); 
	}
	
	@Override public Integer getPort() { 
		return cassandraURI.getPort() == -1 ? null : cassandraURI.getPort(); 
	} // differs from Java URI implementation
	
	@Override public String getPath() {	// returns / or /path
		return (cassandraURI.getPath() == null || cassandraURI.getPath().length() == 0) ? PATH_SEPARATOR : cassandraURI.getPath(); 
	} 
	public CassandraURI withNewPath(String newPath) throws URIException {
		if (newPath == null) throw new IllegalArgumentException("null");
		return new CassandraURI(
			cassandraURI.getScheme() + "://" + 
			cassandraURI.getHost() +
			(cassandraURI.getPort() != -1 ? cassandraURI.getPort() : "") +
			newPath +
			(cassandraURI.getQuery() != null ? "?" + cassandraURI.getQuery() : "") +
			(cassandraURI.getFragment() != null ? "#" + cassandraURI.getFragment() : "")
		); 
	}
	
	@Override public String getURI() { 
		return cassandraURI.toString();
	}
	
	@Override public String getEntryName() { // file, dir, or bucketname (if no path) without termintaing slash
		String tempPath = this.getPath(); 
		if (PATH_SEPARATOR.equals(tempPath)) return tempPath;
		if (tempPath.endsWith(PATH_SEPARATOR)) tempPath = tempPath.substring(0, tempPath.length() - 1); // remove traling slash (dir name)
		return tempPath.substring(tempPath.lastIndexOf('/') + 1, tempPath.length());
	}
	
	Long lastModified; 
	@Override public Long getLastModified() { 
		return lastModified; 
	}
	void setLastModified(Long lastModified) { 
		this.lastModified = lastModified; 
	}

	Long size;
	@Override public Long getSize() { 
		return size; 
	}
	public void setSize(Long size) { 
		this.size = size; 
	}

	String unit = ""; // no unit by default
	@Override public String getSizeUnit() {
		return unit; 
	}
	public void setSizeUnit(String unit) {
		this.unit = unit;
	}
	
	String permissions = "---------"; // default permissions
	@Override public String getPermissions() { 
		return permissions; 
	}
	public void setPermissions(String permissions) { 
		this.permissions = permissions; 
	}

	private String details;
	public void setDetails(String details) { 
		this.details = details; 
	}
	@Override public String getDetails() { 
		return this.details; 
	}
	
	@Override public String getQuery() {
		return cassandraURI.getQuery();
	}

	@Override public String getFragment() {
		return cassandraURI.getFragment();
	}
	
	public String getKeyspace() {
		String tempPath = this.getPath();
		if (PATH_SEPARATOR.equals(tempPath)) return null; // no keyspace
		tempPath = tempPath.substring(1, tempPath.length()); // remove leading /
		if (tempPath.contains(PATH_SEPARATOR)) return tempPath.substring(0, tempPath.indexOf(PATH_SEPARATOR));
		else return tempPath;
	}
	
	public String getTable() {
		String tempPath = this.getPath();
		if (tempPath == null) return null;
		if (PATH_SEPARATOR.equals(tempPath)) return null; // no keyspace
		if (!tempPath.startsWith(PATH_SEPARATOR)) log.warn("Path expected to start with /"); 
		tempPath = tempPath.substring(1, tempPath.length()); // remove leading /
		
		if (!tempPath.contains(PATH_SEPARATOR)) return null; // only keyspace given
		tempPath = tempPath.substring(tempPath.indexOf(PATH_SEPARATOR) + 1); // remove keyspace
		if (tempPath.length() == 0) return null; // /keyspace/
		if (tempPath.endsWith(PATH_SEPARATOR)) return tempPath.substring(0, tempPath.length() - 1); // remove trailing / if applicable
		else return tempPath;
	}
	
	private void print() {
		System.out.println("Protocol: " + getProtocol());
		System.out.println("Host: " + getHost());
		System.out.println("Port: " + getPort());
		System.out.println("Path: " + getPath());
		System.out.println("Entry name: " + getEntryName());
		System.out.println("Type: " + getType());
		System.out.println("Full URI: " + getURI());
		System.out.println("Last modified: " + getLastModified());
		System.out.println("Size: " + getSize());
		System.out.println("Permissions: " + getPermissions());
	}
	
	public static void main(String [] args) throws Exception {
		CassandraURI uri = new CassandraURI("cassandra://host:8080/keyspace/");
		uri.print();
		System.out.println(uri.getKeyspace());
		System.out.println(uri.getTable());
		
	}
}