package hu.sztaki.lpds.dataavenue.adaptors.jsaga;

import org.ogf.saga.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import hu.sztaki.lpds.dataavenue.interfaces.CloseableSessionObject;

public class CloseableJSagaSession implements CloseableSessionObject {
	private static final Logger log = LoggerFactory.getLogger(CloseableJSagaSession.class);
	private final Session jSagaSession;
	
	CloseableJSagaSession(Session jSagaSession) { this.jSagaSession = jSagaSession;	}
	
	Session get() {	return this.jSagaSession; }
	
	@Override public void close() {
		try { 
			jSagaSession.close();
		} catch (Exception e) { 
			log.warn("Cannot close JSAGA session", e); 
		}
	}
}