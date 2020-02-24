package hu.sztaki.lpds.dataavenue.interfaces;

/*
 * Uniform resource identifier (location) for resources accessible by DataAvenue 
 */
public interface URIBase {

	public static final String PATH_SEPARATOR = "/";

	// enum for URI types
	public static enum URIType {URL, FILE, DIRECTORY, SYMBOLIC_LINK, OTHER};
	
	// type of the URI
	public URIType getType();

	// scheme part of the location (e.g., "http")
	public String getProtocol();

	// host part of the URI
	public String getHost();

	// port part of the URI, null if absent
	public Integer getPort();

	// path to resource (including resource name) without query string or fragment (e.g., "/dir/filename.txt")
	public String getPath();
	
	public String getQuery();
	
	public String getFragment();
	
	// name of the file or directory, but without path ("filename.txt") and terminating slash (in the case of dirs)
	public String getEntryName();
	
	// date of last modification in milliseconds since 1/1/1970 GMT, null if unknown
	public Long getLastModified(); 
	
	// size of the resource in bytes, null if unknwon
	public Long getSize();
	
	// unit of size
	public String getSizeUnit();

	// UNIX-style permissions string, null if unknwon
	public String getPermissions();
	
	// string representation of the entire URI
	public String getURI();
	
	// additional comments
	public String getDetails();
}