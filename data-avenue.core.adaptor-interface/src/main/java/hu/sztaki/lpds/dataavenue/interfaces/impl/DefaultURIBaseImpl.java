package hu.sztaki.lpds.dataavenue.interfaces.impl;

import java.net.URI;
import java.net.URISyntaxException;

import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

public class DefaultURIBaseImpl implements URIBase {
	
	private final URI uri; 
	
	public DefaultURIBaseImpl(final URIBase uriParam) throws URIException {
		this(uriParam.getURI());
	}

	public DefaultURIBaseImpl(final String uriParam) throws URIException {
		if (uriParam == null) throw new IllegalArgumentException("null");
		// convert spaces to %20 in URL (otherwise Illegal character exception in URI)
		// TODO convert accented letters éáűőú...
		try { uri = new URI(uriParam.replaceAll(" ", "%20")); } 
		catch (URISyntaxException e) { throw new URIException("Invalid URI: '" + uriParam + "'", e); }
	}
	
	/* default (read-only) URI attributes */
	@Override public String getProtocol() {	
		return uri.getScheme();	
	}
	
	@Override public String getHost() { 
		return uri.getHost(); 
	}
	
	@Override public Integer getPort() { 
		return uri.getPort() == -1 ? null : uri.getPort(); // differs from Java URI implementation 
	} 
	
	@Override public String getPath() {	// returns / or /path without query string
		return (uri.getPath() == null || uri.getPath().length() == 0) ? PATH_SEPARATOR : uri.getPath(); 
	} 
	
	@Override public String getQuery() {
		return uri.getQuery();
	}

	@Override public String getFragment() {
		return uri.getFragment();
	}

	@Override public String getURI() { 
		return uri.toString();
	}
	
	@Override public String toString() { // override toString implementation
		return getURI();
	}
	
	// utils
	@Override public URIType getType() {
		if (uri.getPath() == null || uri.getPath().length() == 0 || PATH_SEPARATOR.equals(uri.getPath())) return URIType.URL;
		else if (uri.getPath().endsWith(PATH_SEPARATOR)) return URIType.DIRECTORY;
		else return URIType.FILE;
	}

	public boolean isFile() {
		return !isDir();
	}
	
	public boolean isDir() {
		return getType() == URIType.DIRECTORY || getType() == URIType.URL; // subdir or root (/)
	}
	
	@Override public String getEntryName() { // file, dir, or bucketname (if no path) without termintaing slash
		String tempPath = this.getPath(); 
		if (PATH_SEPARATOR.equals(tempPath)) return tempPath;
		if (tempPath.endsWith(PATH_SEPARATOR)) tempPath = tempPath.substring(0, tempPath.length() - 1); // remove trailing slash (dir name)
		return tempPath.substring(tempPath.lastIndexOf('/') + 1, tempPath.length());
	}
	
	/* attributes */
	Long lastModified; 
	@Override public Long getLastModified() { 
		return lastModified; 
	}
	public void setLastModified(Long lastModified) { 
		this.lastModified = lastModified; 
	}

	Long size;
	@Override public Long getSize() { 
		return size; 
	}
	public void setSize(Long size) { 
		this.size = size; 
	}

	String unit = "B"; // bytes by default
	@Override public String getSizeUnit() {
		return unit; 
	}
	public void setSizeUnit(String unit) {
		this.unit = unit;
	}
	
	// default permissions, placeholder for drwxrwxrwx
	String permissions = "---------";
	@Override public String getPermissions() {
		if (this.getType() ==  URIType.DIRECTORY) return "d" + permissions;
		else if (this.getType() ==  URIType.SYMBOLIC_LINK) return "l" + permissions;
		else return "-" + permissions; 
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
	
	// copy/move a file or subdir to the same file or subdir? 
	public boolean isIdenticalFileOrDirEntry(URIBase dest) {
		return identicalEntries(this, dest, false);
	}

	// identical files or dirs
	private boolean identicalEntries(URIBase src, URIBase dest, boolean ignoreSrcFileName) {
		if (src == null) return dest == null;
		if (dest == null) return src == null;
		
		if (src.getProtocol() == null) { if (dest.getProtocol() != null) return false; }
		else { if (!getProtocol().equals(dest.getProtocol())) return false; }
		
		if (src.getHost() == null) { if (dest.getHost() != null) return false; }
		else { if (!getHost().equals(dest.getHost())) return false; }

		if (src.getPort() == null) { if (dest.getPort() != null) return false; }
		else { if (!getPort().equals(dest.getPort())) return false; }

		String srcPath = src.getPath();
		if (srcPath != null && srcPath.endsWith("/")) srcPath = srcPath.substring(0, srcPath.length() - 1); // get rid of terminating slash (subdir)
		if (srcPath != null && ignoreSrcFileName) {  
			if (srcPath.contains("/")) srcPath = srcPath.substring(0, srcPath.lastIndexOf('/') + 1); // cut filename or subdirname
			if (srcPath.endsWith("/")) srcPath = srcPath.substring(0, srcPath.length() - 1); // get rid of terminating slash (subdir)
		}
		
		String destPath = dest.getPath();
		if (destPath != null && destPath.endsWith("/")) destPath = destPath.substring(0, destPath.length() - 1); // get rid of terminating slash (subdir)
		
		if (srcPath == null && destPath != null) return false; 
		else return srcPath.equals(destPath);
	}

	//  copy/move a file to its the same subdir? (it causes conflict, as file name will be the same too) 
	public boolean isSameSubdirWithoutFileName(DefaultURIBaseImpl destSubdir) {
		if (destSubdir == null) return false;
		if (!isFile() || !destSubdir.isDir()) return false;
		return identicalEntries(this, destSubdir, true);
	}

	// copy/move a subdir to its subdir?
	public boolean isSubdirOf(DefaultURIBaseImpl src) {
		if (src == null) throw new IllegalArgumentException("null source parameter");
		if (src.getProtocol() == null) throw new IllegalArgumentException("null source protocol");
		if (src.getHost() == null) throw new IllegalArgumentException("null source host");
		
		DefaultURIBaseImpl dest = this;
		
		// not dirs
		if (!src.isDir() || !dest.isDir()) return false; 
		
		// if dest is subdir of src, return true
		if (!src.getProtocol().equals(dest.getProtocol())) return false; // different protocols

		if (!src.getHost().equals(dest.getHost())) return false; // different hosts

		// ignore port info: if (!src.getPort().equals(dest.getPort())) return false; // different ports
		
		String srcPath = src.getPath();
		String destPath  = dest.getPath();
		if (srcPath == null || destPath == null) return false; // silent failover
		return destPath.startsWith(srcPath);
	}
	
	/* 
	 * Returns full path (protocol://host:port/path/) without file or subdirectory name, query string or fragment, but with a slash at the end
	 * protocol://host:port/path/subdir/ -> protocol://host:port/path/
	 * protocol://host:port/path/file.txt -> protocol://host:port/path/
	 */
	public static String getContainerDirUri(URIBase uri) { // returns full path without file or subdir name
		String originalPath = uri.getPath(); // it removes query string and the fragment
		while (originalPath.endsWith("/")) originalPath = originalPath.substring(0, originalPath.length() - 1); // get rid of terminating slash (subdir)
		if (originalPath.contains("/")) originalPath = originalPath.substring(0, originalPath.lastIndexOf('/') + 1); // cut tail (filename or subdir name) from the last slash, keep the rest
		if (!originalPath.endsWith("/")) originalPath += "/"; // add slash to the end
		return (uri.getProtocol() != null ? uri.getProtocol() + "://" : "") + (uri.getHost() != null ? uri.getHost() : "") + (uri.getPort() != null ? ":" + uri.getPort() : "") + originalPath;
	}

}