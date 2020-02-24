package hu.sztaki.lpds.dataavenue.core.rest;

import hu.sztaki.lpds.dataavenue.core.ClientAuthentication;
import hu.sztaki.lpds.dataavenue.core.TicketManager;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.TicketException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status;

@Path("/keys") 
public class Keys {
	private static final Logger log = LoggerFactory.getLogger(Keys.class); 

	private void logRequest(final String method, final HttpHeaders httpHeaders, final HttpServletRequest request) {
		String key = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY).get(0) : null;
		String uri = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI).get(0) : null;
		log.info("REST " + method + ", key: " + key + ", URI: " + uri + ", from: " + request.getRemoteAddr());
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public Response create(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			String entityBody) {
		try {
		logRequest("POST", headers, request);

		String key;
		try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
		catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
        
        JSONObject options = new JSONObject();
        if (entityBody != null && entityBody.length() > 0) {
    		log.debug(entityBody);
    		try { options = new JSONObject(new JSONTokener(entityBody)); }
    		catch (JSONException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
    		log.debug(options.toString(4));
        }
		
        String name = "";
        try { name = options.getString("name"); }
        catch (JSONException e) {} // no such key or invalid

        String company = "";
        try { company = options.getString("company"); }
        catch (JSONException e) {} // no such key or invalid

        String email = "";
        try { company = options.getString("email"); }
        catch (JSONException e) {} // no such key or invalid
        
    	try {
        	String result = TicketManager.getInstance().createUserTicket(key, name, company, email);
        	return Response.status(Status.OK).entity(result).build();
    	}
    	catch (TicketException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	    catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();  }
        
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}

}
