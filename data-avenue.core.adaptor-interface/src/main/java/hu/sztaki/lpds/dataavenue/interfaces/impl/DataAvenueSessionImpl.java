package hu.sztaki.lpds.dataavenue.interfaces.impl;

import java.util.Hashtable;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import hu.sztaki.lpds.dataavenue.interfaces.CloseableSessionObject;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;

@SuppressWarnings("serial")
public class DataAvenueSessionImpl extends Hashtable<String, Object> implements DataAvenueSession {
	public static final String DATA_AVENUE_SESSION_KEY = "dataavenue_session";
    public final static String TICKET_SESSION_KEY = "ticket";

	public void discard() {
		for (Object o: values()) {
			if (o instanceof CloseableSessionObject) {
				((CloseableSessionObject)o).close();
			}
		}
		clear();
	}
}