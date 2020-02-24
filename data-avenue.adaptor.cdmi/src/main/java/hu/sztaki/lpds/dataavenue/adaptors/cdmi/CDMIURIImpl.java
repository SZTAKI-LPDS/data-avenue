package hu.sztaki.lpds.dataavenue.adaptors.cdmi;

import java.net.URI;
import java.net.URISyntaxException;

import hu.sztaki.lpds.cdmi.api.CDMIConstants;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import static hu.sztaki.lpds.dataavenue.interfaces.URIBase.URIType.*;

public class CDMIURIImpl implements URIBase {
	private final URI uri; // used to store scheme, host, port parts
	
	public CDMIURIImpl(String cdmiUri) throws URIException { // cdmi://host[:port][/path/fileordir[/]]
		if (cdmiUri == null) throw new IllegalArgumentException("null argument");
		try { uri = new URI(cdmiUri).normalize(); } 
		catch (URISyntaxException e) { throw new URIException("URI is of invalid format: " + e.getMessage()); }
		if (uri.getScheme() == null) throw new URIException("Missing protocol name");
		if (!CDMIAdaptor.CDMI_PRPOTOCOL.equals(uri.getScheme()) && !CDMIAdaptor.CDMIS_PRPOTOCOL.equals(uri.getScheme())) throw new URIException("Invalid protocol: " + uri.getScheme() + "");
		if (uri.getHost() == null) throw new URIException("Missing host name");
		if (uri.getQuery() != null) throw new URIException("Query string not allowed");
		if (uri.getFragment() != null) throw new URIException("Fragment not allowed");
		if (uri.getPath() == null || uri.getPath().length() == 0 || uri.getPath().endsWith(CDMIConstants.PATH_SEPARATOR)) type = DIRECTORY;
		else type = FILE;
	}
	
	final URIType type;
	@Override public URIType getType() { return type; }
	@Override public String getProtocol() {	return uri.getScheme();	}
	@Override public String getHost() { return uri.getHost(); }
	@Override public Integer getPort() { return uri.getPort() != -1 ? uri.getPort() : null; } // differs from Java URI implementation
	@Override public String getPath() {	return (uri.getPath() == null || uri.getPath().length() == 0) ? CDMIConstants.PATH_SEPARATOR : uri.getPath(); } 
	@Override public String getURI() { return getProtocol() + "://" + getHost() + (getPort() != null ? ":" + getPort() : "") + getPath(); }
	public String getHTTPURI() { return (CDMIAdaptor.CDMI_PRPOTOCOL.equals(uri.getScheme()) ? "http://" : "https://") + getHost() + (getPort() != null ? ":" + getPort() : "") + getPath(); }
	
	@Override public String getEntryName() { // file, dir, or bucketname (if no path) without termintaing slash
		String tempPath = this.getPath(); 
		if (tempPath.endsWith(CDMIConstants.PATH_SEPARATOR)) tempPath = tempPath.substring(0, tempPath.length() - 1); // remove traling slash (dir name)
		return tempPath.substring(tempPath.lastIndexOf('/') + 1, tempPath.length());
	}
	
	Long lastModified; 
	@Override public Long getLastModified() { return lastModified; }
	void setLastModified(Long lastModified) { this.lastModified = lastModified; }

	Long size;
	@Override public Long getSize() { return size; }
	public void setSize(Long size) { this.size = size; }

	String permissions = "---------"; // default permissions
	@Override public String getPermissions() { return permissions; }
	public void setPermissions(String permissions) { this.permissions = permissions; }

	private String details;
	public void setDetails(String details) { this.details = details; }
	@Override public String getDetails() { return this.details; }

	public void print() {
		System.out.println("Protocol: " + getProtocol());
		System.out.println("Host: " + getHost());
		System.out.println("Port: " + getPort());
		System.out.println("Path: " + getPath() + "");
		System.out.println("Entry name: " + getEntryName() + "");
		System.out.println("Type: " + getType());
		System.out.println("Full URI: " + getURI());
		System.out.println("Last modified: " + getLastModified());
		System.out.println("Size: " + getSize());
		System.out.println("Permissions: " + getPermissions());
	}
	
	@Override
	public String getQuery() {
		return null;
	}
	@Override
	public String getFragment() {
		return null;
	}
	@Override
	public String getSizeUnit() {
		return null;
	}
}