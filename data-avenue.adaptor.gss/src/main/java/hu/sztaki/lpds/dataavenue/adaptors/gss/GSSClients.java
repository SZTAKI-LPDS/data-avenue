package hu.sztaki.lpds.dataavenue.adaptors.gss;

import java.util.HashMap;
import java.util.Map;

import hu.sztaki.lpds.dataavenue.interfaces.CloseableSessionObject;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// this class stores clients created earlier for different hosts (contained by URIs) and returns to save resources and improve efficiency
class GSSClients implements CloseableSessionObject {
	
	private static final Logger log = LoggerFactory.getLogger(GSSClients.class);
	
	// map: host -> client
	private final Map<String, GSSClient> clients = new HashMap<String, GSSClient>(); 
	
	void add(final URIBase uri, GSSClient client) {
		String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
		clients.put(hostAndPort, client);
	}

	GSSClient get(final URIBase uri) {
		String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
		GSSClient client = clients.get(hostAndPort); 
		return client;
	}
	
	@Override public void close() {
		for (GSSClient client: clients.values()) {
			try { client.shutdown(); } 
			catch (Exception e) { log.warn("Cannot shutdown client", e); }
		}
	}
}