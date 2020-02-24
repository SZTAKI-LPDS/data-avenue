package hu.sztaki.lpds.dataavenue.core.interfaces.impl;

import org.ogf.saga.error.BadParameterException;
import org.ogf.saga.error.NoSuccessException;
import org.ogf.saga.url.URL;
import org.ogf.saga.url.URLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

/**
 * Generic URI of a resource based on jSAGA URL
 * To be used in JSaga adapter only
 * Use DefaultURIBaseImpl anywhere else
 */
@Deprecated
public abstract class URIImpl implements URIBase {
	private static final Logger log = LoggerFactory.getLogger(URIImpl.class);
	static { 
		
		// setting 'saga.factory' is required, to allow use of URIImple without JSagaGenericAdaptor (where it is also set) 
		try { // it should happen before any jSAGA call, otherwise No SAGA factory name specified exception
			if (System.getProperty("saga.factory") == null) {
				System.setProperty("saga.factory", "fr.in2p3.jsaga.impl.SagaFactoryImpl");
				log.info("System property 'saga.factory' set to 'fr.in2p3.jsaga.impl.SagaFactoryImpl'");
			}
		} catch (SecurityException x) {	log.error("Cannot set system property 'saga.factory'"); }
	}
	
	// URL in jSAGA (supports more protocols than java.net.URL) 
	private final URL jSagaUrl;
	public URL getJSagaUrl() { return jSagaUrl;	}
	
	protected URIImpl(final String url) throws URIException {
		if (url == null) throw new URIException("No value specified for source or target URI (null)");
		jSagaUrl = toJSagaURL(url)/*.normalize()*/;
	}
	
	public static URL toJSagaURL(final String url)  throws URIException {
		try { return URLFactory.createURL(url);	} 
		catch (BadParameterException e) { throw new URIException("Invalid URL: " + url + "(" + e.getMessage() + ")", e); } 
		catch (NoSuccessException e) { throw new URIException("Cannot create URL for: " + url + "(" + e.getMessage() + ")", e);	}
	}
	
	// remove redundant slashes
	private String normalize(String urlWithDoubleSlashes) {
		// do not normalize in this way!
//		if (urlWithDoubleSlashes == null) return null;
//		while (urlWithDoubleSlashes.contains("//")) urlWithDoubleSlashes = urlWithDoubleSlashes.replaceAll("//", "/");
		return urlWithDoubleSlashes;
	}

	// copy/move a file or subdir to the same file or subdir? 
	public boolean isIdenticalFileOrDirEntry(URIImpl dest) {
		return identicalEntries(this, dest, false);
	}

	//  copy/move a file to its the same subdir? (it causes conflict, as file name will be the same too) 
	public boolean isSameSubdirWithoutFileName(URIImpl destSubdir) {
		if (destSubdir == null) return false;
		if (!isFile() || !destSubdir.isDir()) return false;
		return identicalEntries(this, destSubdir, true);
	}
		
	// copy/move a subdir to its subdir?
	public boolean isSubdirOf(URIImpl src) {
		if (src == null) throw new IllegalArgumentException("null source parameter");
		if (src.getProtocol() == null) throw new IllegalArgumentException("null source protocol");
		if (src.getHost() == null) throw new IllegalArgumentException("null source host");
		
		URIImpl dest = this;
		
		// not dirs
		if (!src.isDir() || !dest.isDir()) return false; 
		
		// if dest is subdir of src, return true
		if (!src.getProtocol().equals(dest.getProtocol())) return false; // different protocols

		if (!src.getHost().equals(dest.getHost())) return false; // different hosts

		// ignore port info: if (!src.getPort().equals(dest.getPort())) return false; // different ports
		
		String srcPath = normalize(src.getPath());
		String destPath  = normalize(dest.getPath());
		if (srcPath == null || destPath == null) return false; // silent failover
		return destPath.startsWith(srcPath);
	}
	
	// identical files or dirs
	private boolean identicalEntries(URIImpl src, URIImpl dest, boolean ignoreSrcFileName) {
		if (src == null) return dest == null;
		if (dest == null) return src == null;
		
		if (src.getProtocol() == null) { if (dest.getProtocol() != null) return false; }
		else { if (!getProtocol().equals(dest.getProtocol())) return false; }
		
		if (src.getHost() == null) { if (dest.getHost() != null) return false; }
		else { if (!getHost().equals(dest.getHost())) return false; }

		if (src.getPort() == null) { if (dest.getPort() != null) return false; }
		else { if (!getPort().equals(dest.getPort())) return false; }

		String srcPath = normalize(src.getPath());
		if (srcPath != null && srcPath.endsWith("/")) srcPath = srcPath.substring(0, srcPath.length() - 1); // get rid of terminating slash (subdir)
		if (srcPath != null && ignoreSrcFileName) {  
			if (srcPath.contains("/")) srcPath = srcPath.substring(0, srcPath.lastIndexOf('/') + 1); // cut filename or subdirname
			if (srcPath.endsWith("/")) srcPath = srcPath.substring(0, srcPath.length() - 1); // get rid of terminating slash (subdir)
		}
		
		String destPath  = normalize(dest.getPath());
		if (destPath != null && destPath.endsWith("/")) destPath = destPath.substring(0, destPath.length() - 1); // get rid of terminating slash (subdir)
		
		if (srcPath == null && destPath != null) return false; 
		else return srcPath.equals(destPath);
	}

	@Override public URIType getType() { return URIType.URL; }

	@Override public String getProtocol() { return jSagaUrl.getScheme(); }

	@Override public String getHost() {	return jSagaUrl.getHost(); }

	@Override public Integer getPort() { return jSagaUrl.getPort() != -1 ? jSagaUrl.getPort() : null; }

	@Override public String getPath() { return jSagaUrl.getPath(); }
	
	@Override public String getQuery() { return jSagaUrl.getQuery(); }

	@Override public String getFragment() {	return jSagaUrl.getFragment(); }
	
	@Override public String getEntryName() {
		String path = jSagaUrl.getPath();
		if (path == null) return "";
		while (path.endsWith("/")) path = path.substring(0, path.length() - 1); // remove slash tail (directory names)
		if (!path.contains("/")) { return ""; } // no name, only host	
		return path.substring(path.lastIndexOf('/') + 1, path.length());
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
	
	@Override public Long getLastModified() { return null; } // n.a. by default

	@Override public Long getSize() { return null; } // n.a. by default
	
	String unit = "B"; // bytes by default
	@Override public String getSizeUnit() {
		return unit; 
	}
	public void setSizeUnit(String unit) {
		this.unit = unit;
	}
	
	// returns the full URI
	@Override public String getURI() { return jSagaUrl.getString(); }

	public Boolean isDir() { return this.getURI().endsWith("/"); }

	public Boolean isFile() { return !isDir(); }
	
	private String permissions = "-rw------"; // owner/group/others
	@Override public String getPermissions() { return this.permissions;	}

	private String details;
	public void setDetails(String details) { this.details = details; }
	@Override public String getDetails() { return this.details; }
	
	public static void main(String [] args) throws Exception{
		URIBase uri = URIFactory.createURI("http://grid.in2p3.fr/maven2/fr/in2p3/jsaga/jsaga-engine/0.9.17-SNAPSHOT");
		System.out.println(uri.getPath());

		uri = URIFactory.createURI("http://grid.in2p3.fr:900");
		System.out.println(uri.getURI());
		
	}
}
