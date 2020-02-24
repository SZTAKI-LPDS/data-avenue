package hu.sztaki.lpds.dataavenue.core.rest;

import hu.sztaki.lpds.dataavenue.core.AdaptorRegistry;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status;

@Path("/protocols") 
public class Protocols {
	
	private static final Logger log = LoggerFactory.getLogger(Protocols.class); 
	
	private void logRequest(final String method, final HttpServletRequest request) {
		log.info("REST " + method + ", from: " + request.getRemoteAddr());
	}
	
	@GET
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response list(
			@Context HttpServletRequest request) {
		try {
		logRequest("GET", request);
    	
        JSONArray jsonArray = new JSONArray();
    	for (String i : AdaptorRegistry.getSupportedProtocols()) jsonArray.put(i);
		
    	log.debug(jsonArray.toString(4));
		
		return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonArray.toString()).build();

		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
}