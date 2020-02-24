package hu.sztaki.lpds.dataavenue.core.rest;

import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class HttpSessionUtils {
//	private static final Logger log = LoggerFactory.getLogger(HttpSessionUtils.class);
	
	// returns a Data Avenue session object:
	// - new, empty, if no HTTP session request,
	//   otherwise (header contains X-Use-Session or Cookie: JSESSIONID=)
	// - a previously created Data Avenue session stored in HTTP session
	// - new Data Avenue session, which will also be stored in HTTP session 
	static DataAvenueSession getSession(final HttpHeaders httpHeaders, final HttpServletRequest request) {
        DataAvenueSession session = null;
        /*
        boolean useSession = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_USE_SESSION) != null; 
        if (useSession || request.getSession(false) != null) { // if session requested (X-Use-Session) or session requested by ID (Cookie: JSESSIONID=)
        	HttpSession httpSession = request.getSession(false); // get existing session or null
        	if (httpSession == null) { // no session yet
        		log.debug("Creating new HTTP session...");
        		httpSession = request.getSession(true); // create a new session
        	} else {
        		if (request.isRequestedSessionIdValid()) {
            		log.debug("Using existing HTTP session...");
        			session = (DataAvenueSession) httpSession.getAttribute(DataAvenueSessionImpl.DATA_AVENUE_SESSION_KEY);
        		} else {
        			log.debug("Session expired! Creating new HTTP session.");
        			// httpSession = request.getSession(true); // create a new session
        		}
        	}
    		if (session == null) {
    			session = new DataAvenueSessionImpl();
    			httpSession.setAttribute(DataAvenueSessionImpl.DATA_AVENUE_SESSION_KEY, session);
    		}        
    	} else { // no HTTP session
    		session = new DataAvenueSessionImpl();
    	}*/
        return session;
	}
	
//	static void discardSession(final HttpHeaders httpHeaders, final HttpServletRequest request, final DataAvenueSession session) {
		// nothing to discard, as no session maintined in REST
//		return;
//		boolean useSession = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_USE_SESSION) != null;
//		if (session != null && !useSession && request.getSession(false) == null && session instanceof DataAvenueSessionImpl) 
//			((DataAvenueSessionImpl)session).discard();
//	}
//	static boolean isSessionDiscardable(final HttpHeaders httpHeaders, final HttpServletRequest request, final DataAvenueSession session) {
//		boolean useSession = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_USE_SESSION) != null;
//		if (session != null && !useSession && request.getSession(false) == null && session instanceof DataAvenueSessionImpl) 
//			return true;
//		else
//			return false;
//	}
}