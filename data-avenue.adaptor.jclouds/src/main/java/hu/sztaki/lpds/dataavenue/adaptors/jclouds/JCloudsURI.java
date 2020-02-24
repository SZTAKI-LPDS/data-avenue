package hu.sztaki.lpds.dataavenue.adaptors.jclouds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

public class JCloudsURI extends DefaultURIBaseImpl {
	
	public static final String PATH_SEPARATOR = "/";
	
	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger(JCloudsURI.class);
	
	JCloudsURI(final URIBase uri) throws URIException { super(uri);	}
	JCloudsURI(final String uriString) throws URIException { super(uriString);	}
	
	static String getAuthEndpoint(final URIBase uri, final String protocol, final String authPrefix) {
		String authPostfix = authPrefix != null ? (authPrefix.startsWith("/") ? "" : "/") + authPrefix : ""; 
		String result = (protocol != null ? protocol : "https")	+ "://" 
				+ uri.getHost()	+ (uri.getPort() != null ? ":" + uri.getPort() : "") 
				+ authPostfix; // starts with / or empty string
		return result; 
	}

	String getContainerName() { 
		String tempPath = this.getPath();
		if (PATH_SEPARATOR.equals(tempPath)) return null; 
		if (tempPath.length() == 0) return null;
		if (tempPath.startsWith(PATH_SEPARATOR)) tempPath = tempPath.substring(1, tempPath.length()); // remove leading /
		if (tempPath.length() == 0) return null;
		// now container name starts here up to next /; if not terminated by /, this is a file out of any container
		String result;
		if (tempPath.contains(PATH_SEPARATOR)) {
			result = tempPath.substring(0, tempPath.indexOf(PATH_SEPARATOR));
		} else {
			// container name without terminating / is a file
			return null;
		}
		return result;
	}

	// returns dir path within a container
	// - null: no container or container only
	// - path of dir without leading and trailing /, e.g.: dir1/dir2/dir3: in the case of dirs and files
	String getDirectoryPath() { // directory path without container name
		if (getContainerName() == null) return null;
		String tempPath = this.getPath();
		if (tempPath.startsWith(PATH_SEPARATOR)) tempPath = tempPath.substring(1); // remove leading /
		tempPath = tempPath.substring(getContainerName().length()); // cut container prefix
		if (tempPath.startsWith(PATH_SEPARATOR)) tempPath = tempPath.substring(1); // remove leading /
		if (tempPath.length() == 0 || !tempPath.contains(PATH_SEPARATOR)) return null;  
		String result = (tempPath.contains(PATH_SEPARATOR)) ? tempPath.substring(0, tempPath.lastIndexOf(PATH_SEPARATOR)) : tempPath; // the path till last
		return result;
	}
	
	// returns parent directory if uri is a dir, otherwise the directory containing the file
	public URIBase getParent() throws URIException {
		String container = getContainerName(); 
		String dirPath = getDirectoryPath();
		String newPath;
		if (container == null) {
			if (getType() != URIType.DIRECTORY) newPath = "";
			else throw new URIException("Root has no parent");
		} else if (getType() == URIType.FILE || getType() == URIType.SYMBOLIC_LINK) {
			if (dirPath != null) newPath = getContainerName() + URIBase.PATH_SEPARATOR + dirPath + URIBase.PATH_SEPARATOR;
			else newPath = getContainerName() + URIBase.PATH_SEPARATOR;
		} else { // directory
			if (dirPath == null) newPath = "";
			else if (!dirPath.contains(URIBase.PATH_SEPARATOR)) newPath = getContainerName() + URIBase.PATH_SEPARATOR;  
			else newPath = getContainerName() + URIBase.PATH_SEPARATOR + dirPath.substring(0, dirPath.lastIndexOf(URIBase.PATH_SEPARATOR)) + URIBase.PATH_SEPARATOR; // keep until last /
		}
		return new DefaultURIBaseImpl(getProtocol() + "://" + getHost() + (getPort() != null ? ":" + getPort() : "") + "/" + newPath);
	}	
}