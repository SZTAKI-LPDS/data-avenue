package hu.sztaki.lpds.dataavenue.interfaces;

import java.util.Map;

/*
 * This class represents a session object preserved between different service invocations originating from the same client.
 * This session is passed to adaptor methods and persisted by Data Avenue as long as the session lives. 
 * Wrapping session objects into CloseableSessionObject guarantees that its close() method will be invoked on session timeout (release resources). 
 */
public interface DataAvenueSession extends Map<String, Object> {}