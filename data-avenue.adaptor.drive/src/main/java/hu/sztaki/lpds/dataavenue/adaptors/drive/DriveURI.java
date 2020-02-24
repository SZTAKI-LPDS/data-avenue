package hu.sztaki.lpds.dataavenue.adaptors.drive;

import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

public class DriveURI extends DefaultURIBaseImpl {
	
	DriveURI(final URIBase uri) throws URIException {
		this(uri.getURI());
	}

	DriveURI(final String uri) throws URIException {
		super(uri);
		if (!DriveAdaptor.PROTOCOLS.contains(getProtocol())) throw new URIException("Invalid protocol!");
	}

	String getContainer() { // bucket
		String tempPath = this.getPath();
		if (PATH_SEPARATOR.equals(tempPath)) return null; // no keyspace
		tempPath = tempPath.substring(1, tempPath.length()); // remove leading /
		if (tempPath.contains(PATH_SEPARATOR)) return tempPath.substring(0, tempPath.indexOf(PATH_SEPARATOR));	
		else return tempPath;
	}
	String getName() { // path without bucket name
		String tempPath = this.getPath();
		if (PATH_SEPARATOR.equals(tempPath)) return null; // no keyspace
		tempPath = tempPath.substring(1, tempPath.length()); // remove leading /
		if (tempPath.contains(PATH_SEPARATOR) && (tempPath.indexOf(PATH_SEPARATOR) + 1 < tempPath.length()) ) return tempPath.substring(tempPath.indexOf(PATH_SEPARATOR) + 1);	
		else return null;
	}

	DriveURI withNewPath(final String newPath) throws URIException {
		if (newPath == null) throw new IllegalArgumentException("null");
		return new DriveURI(
			getProtocol() + "://" + 
			getHost() +
			(getPort() != null ? getPort() : "") +
			newPath +
			(getQuery() != null ? "?" + getQuery() : "") +
			(getFragment() != null ? "#" + getFragment() : "")
		); 
	}
}