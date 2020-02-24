package hu.sztaki.lpds.dataavenue.core.interfaces.impl;

import java.net.URI;
import java.net.URISyntaxException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

/*
 * Use new DefaultURIBaseImpl(...)
 */
@Deprecated
public class URIFactory {
	public static URIImpl createURI(final String uriString) throws URIException {
		if (uriString == null || uriString.length() == 0) throw new URIException("Missing URI!");
		URI uri;
		try { uri = new URI(uriString); /*.normalize();*/ } // do not normalize 
		catch (URISyntaxException e) { throw new URIException("URI syntax error: " + e.getMessage()); }
		
		if (uri.getScheme() == null) throw new URIException("Missing protocol name!");
		if (uri.getHost() == null) throw new URIException("Missing host name!");
//		if (uri.getQuery() != null) throw new URIException("Query string not supported!"); cassandra plugin uses it
//		if (uri.getFragment() != null) throw new URIException("Fragment not supported!"); cassandra plugin uses it
		if (uri.getPath() != null && uri.getPath().length() > 1 && !uri.getPath().startsWith("/")) { throw new URIException("URI path must begin with /"); }

		return (uri.getPath() == null || "".equals(uri.getPath()) || "/".equals(uri.getPath()) || (uri.getPath().length() > 1 && uri.getPath().endsWith("/"))) ?
			new DirEntryImpl(uriString) :
			new FileEntryImpl(uriString);
	}
}
