package hu.sztaki.lpds.dataavenue.adaptors.s3;


import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

import static hu.sztaki.lpds.dataavenue.interfaces.URIBase.URIType.*;

/*
 * Permitted formats:
 * s3://host			(https)
 * s3://host:80 		(use http)
 * s3://host:443
 * s3://host:443/
 * s3://host/bucket
 * s3://host/bucket/
 * s3://host/bucket/folder/
 * s3://host/bucket/file
 */
public class S3URIImpl implements URIBase {
	
	private final URIBase uri; // used to store scheme, host, port parts
	static final String DELIMITER = "/";
	
	private String bucketName;
	String getBucketName() { return this.bucketName; }
	void setBucketName(final String bucketName) { this.bucketName = bucketName; }
	
	private String pathWithinBucket; // path without bucket name: <bucketName>/path, starts with /
	String getPathWithinBucket() { return pathWithinBucket; }
	void setPathWithinBucket(final String pathWithinBucket) { this.pathWithinBucket = pathWithinBucket; }

	public S3URIImpl(String s3Uri) throws URIException { // s3://host[:port][/bucketname/dir/fileordir[/]]

		if (s3Uri == null) throw new IllegalArgumentException("null argument");
		
		
		uri = new DefaultURIBaseImpl(s3Uri);
		
		if (uri.getProtocol() == null) throw new URIException("Missing protocol name");
		if (uri.getHost() == null) throw new URIException("Missing host name");
		if (uri.getQuery() != null) throw new URIException("Query string not allowed");
		if (uri.getFragment() != null) throw new URIException("Fragment not allowed");

		// uri.getPath() == "" if no path provided
		if (uri.getPath() == null || uri.getPath().length() <= 1) { // no bucketname: "s3://host", "s3://host/")
			if (uri.getPath() != null) {
				if (!"".equals(uri.getPath()) && !uri.getPath().startsWith(DELIMITER)) throw new URIException("Path must begin with slash"); // should not happen
				// path is either null, 0-long or the / character
			}
			bucketName = null;
			setPathWithinBucket(null);
			type = URL; // root directory
		} else { // path starts with '/' followed by a bucket name
			if (!uri.getPath().startsWith(DELIMITER)) throw new URIException("Path must begin with slash"); // should not happen 
			String pathWithBucketName = uri.getPath().substring(1); // at least one-character long
			if (!pathWithBucketName.contains(DELIMITER)) { // only bucketname
				setBucketName(pathWithBucketName);
				setPathWithinBucket(null);
				type = DIRECTORY;
			} else if (pathWithBucketName.indexOf(DELIMITER) == pathWithBucketName.length() - 1) { // bucketname/
				setBucketName(pathWithBucketName.substring(0, pathWithBucketName.indexOf(DELIMITER)));
				setPathWithinBucket(null);
				type = DIRECTORY;
			} else {
				bucketName = pathWithBucketName.substring(0, pathWithBucketName.indexOf(DELIMITER)); // bucketname ends at first slash	
				pathWithinBucket = pathWithBucketName.substring(pathWithBucketName.indexOf(DELIMITER)); // pathWithinBucket starts with slash
				type = pathWithinBucket.endsWith(DELIMITER)? DIRECTORY : FILE;
			}
		}
	}
	
	final URIType type;
	@Override public URIType getType() { return type; }
	@Override public String getProtocol() { return uri.getProtocol(); }
	@Override public String getHost() { return uri.getHost(); }
	@Override public Integer getPort() { return uri.getPort() != -1 ? uri.getPort() : null; } // differs from Java URI implementation
	@Override public String getPath() {	return uri.getPath(); } // returns full path with bucketname
	@Override public String getURI() { return uri.toString(); }
	
	@Override public String getEntryName() { // file, dir, or bucketname (if no path) without termintaing slash
		if (getPathWithinBucket() == null) return bucketName != null ? bucketName : null;
		String tempPath = getPathWithinBucket(); // path starts with: '/'
		if (tempPath.endsWith(DELIMITER)) tempPath = tempPath.substring(0, tempPath.length() - 1); // remove slash tail (directory names)
		return tempPath.substring(tempPath.lastIndexOf('/') + 1, tempPath.length());
	}
	
	Long lastModified; 
	@Override public Long getLastModified() { return lastModified; }
	void setLastModified(Long lastModified) { this.lastModified = lastModified; }

	Long size;
	@Override public Long getSize() { return size; }
	public void setSize(Long size) { this.size = size; }

	String unit = "B"; // bytes by default
	@Override public String getSizeUnit() { return unit; }
	public void setSizeUnit(String unit) { this.unit = unit; }
	
	String permissions = "---------"; // default permissions
	@Override public String getPermissions() {
		if (this.getType() == DIRECTORY) return "d" + permissions;
		else if (this.getType() == SYMBOLIC_LINK) return "l" + permissions; 
		else return "-" + permissions; 
	}
	public void setPermissions(String permissions) { 
		this.permissions = permissions;
	}

	private String details;
	public void setDetails(String details) { this.details = details; }
	@Override public String getDetails() { return this.details; }

	void print() {
		System.out.println("Full URI: " + getURI());
		System.out.println("Protocol: " + getProtocol());
		System.out.println("Host: " + getHost());
		System.out.println("Port: " + getPort());
		System.out.println("Path: '" + getPath() + "'");
		System.out.println();
		System.out.println("Bucket: " + getBucketName());
		System.out.println("Path within bucket: " + getPathWithinBucket());
		System.out.println("Entry name: " + getEntryName());
		System.out.println("Type: " + getType());
		System.out.println("Last modified: " + getLastModified());
		System.out.println("Size: " + getSize());
		System.out.println("Permissions: " + getPermissions());
	}
	
	public static void main(String [] args) throws Exception {
		S3URIImpl u = new S3URIImpl("s3://192.168.0.198:80");
		System.out.println("protocol: " + u.getProtocol());
		System.out.println("Host: " + u.getHost());
		System.out.println("Port: " + u.getPort());
		System.out.println("Path: '" + u.getPath() + "'");
		System.out.println("Type: " + u.getType());
		System.out.println("Full URI: " + u.getURI());
		System.out.println("Last modified: " + u.getLastModified());
		System.out.println("Size: " + u.getSize());
		System.out.println("Permissions: " + u.getPermissions());
		System.out.println();
		System.out.println("Bucket: " + u.getBucketName());
		System.out.println("Path within bucket: " + u.getPathWithinBucket());
		System.out.println("Entry name: " + u.getEntryName());
	}
	@Override
	public String getQuery() {
		return getQuery();
	}
	@Override
	public String getFragment() {
		return getFragment();
	}
	
	public S3URIImpl getParent() throws URIException {
		String parentURI = this.getURI();
		if (getBucketName() == null) {
			// if no bucket name, return itself
		} else if (getPathWithinBucket() == null) { // only bucket name
			if (parentURI.endsWith(URIBase.PATH_SEPARATOR)) parentURI = parentURI.substring(0, parentURI.length() - 1);
			parentURI = parentURI.substring(0, parentURI.lastIndexOf(URIBase.PATH_SEPARATOR)) + URIBase.PATH_SEPARATOR;
		} else {
			if (getType() == URIType.FILE) { // if file, parent dir is the URI without file name
				parentURI = parentURI.substring(0, parentURI.lastIndexOf(URIBase.PATH_SEPARATOR)) + URIBase.PATH_SEPARATOR;
			} else { // if dir, remove subdir name
				if (parentURI.endsWith(URIBase.PATH_SEPARATOR)) parentURI = parentURI.substring(0, parentURI.length() - 1);
				parentURI = parentURI.substring(0, parentURI.lastIndexOf(URIBase.PATH_SEPARATOR)) + URIBase.PATH_SEPARATOR;
			}
		} 
		return new S3URIImpl(parentURI);
	}
}
