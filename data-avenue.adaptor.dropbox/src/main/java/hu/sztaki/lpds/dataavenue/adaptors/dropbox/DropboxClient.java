package hu.sztaki.lpds.dataavenue.adaptors.dropbox;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;

public class DropboxClient  {

private final static Map<String, DbxClientV2> clients = new HashMap<String, DbxClientV2>(); // map: host -> client
	
	// Get the client from the dropbox, authentication process
	DropboxClient withClient(final URIBase uri, final String user, final String access) throws IOException, GeneralSecurityException {
		DbxRequestConfig config = new DbxRequestConfig("dropbox/" + user); // FIXME ?
		String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
		clients.put(hostAndPort, new DbxClientV2(config, access));
		return this;
	}
	
	// Get back the client with the uri
	DbxClientV2 get(final URIBase uri) {
		String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
		return clients.get(hostAndPort);
	}
}
