package hu.sztaki.lpds.dataavenue.core.webservices;

import hu.sztaki.lpds.dataavenue.interfaces.impl.DataAvenueSessionImpl;

import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class WebServiceSessionListener implements HttpSessionListener {
	private static final Logger log = LoggerFactory.getLogger(WebServiceSessionListener.class);
	
	@Override public void sessionCreated(HttpSessionEvent arg0) {
		if (arg0 == null) { log.warn("No session"); return; }
//		HttpSession httpSession = arg0.getSession();
//		if (httpSession != null) log.debug("" + httpSession.getId());
//		else log.warn("No HTTP session");
	}

	@Override public void sessionDestroyed(HttpSessionEvent arg0) {
		if (arg0 == null) { log.warn("No session"); return; }
		
		HttpSession httpSession = arg0.getSession();
		if (httpSession != null)  {
//			log.debug("" + httpSession.getId());
			DataAvenueSessionImpl dataAvenueSession = (DataAvenueSessionImpl) httpSession.getAttribute(DataAvenueSessionImpl.DATA_AVENUE_SESSION_KEY);
	    	if (dataAvenueSession != null) {
//	    		log.trace("Discarding DataAvenue session...");
	    		try { dataAvenueSession.discard(); } 
	    		catch(Throwable e) { log.warn("Cannot close DataAvenue session", e); }
	    	}
		} else log.warn("No HTTP session"); 
	}
}