package hu.sztaki.lpds.dataavenue.core;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.TicketException;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.UnexpectedException;

public class ClientAuthentication {
	private static final Logger log = LoggerFactory.getLogger(ClientAuthentication.class); 

	public static String getAuthenticatedId(final String token, final HttpServletRequest request) throws TicketException, UnexpectedException {
		String userId;

		if (token == null || "".equals(token)) throw new TicketException("Authentication failed! (Missing DataAvenue server KEY. Check Settings tab on UI or HTTP headers.)"); 

		// x-key authentication
		TicketManager.getInstance().checkTicket(token);  
		userId = token;
		log.debug("TICKET authentication. Client id: " + token);
		
		return userId;
	}
}
