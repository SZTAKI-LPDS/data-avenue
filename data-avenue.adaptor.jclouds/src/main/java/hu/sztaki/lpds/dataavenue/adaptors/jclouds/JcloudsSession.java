package hu.sztaki.lpds.dataavenue.adaptors.jclouds;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.interfaces.CloseableSessionObject;

public class JcloudsSession implements CloseableSessionObject {
	
	private static final Logger log = LoggerFactory.getLogger(JcloudsSession.class);
	private final Map<String, BlobStoreContext> sessions = new ConcurrentHashMap<String, BlobStoreContext>(); // map: host -> blobstore context
	
	
	void put(final String hostAndPort, final BlobStoreContext context) {
		sessions.put(hostAndPort, context);
	}
	
	BlobStoreContext get(final String hostAndPort) {
		return sessions.get(hostAndPort);
	}
	
	@Override
	public void close() {
		for (BlobStoreContext session: sessions.values()) {
			try { session.close(); } catch (Exception x) { log.warn("Cannot close blobstore context", x); }
		}
	}
}