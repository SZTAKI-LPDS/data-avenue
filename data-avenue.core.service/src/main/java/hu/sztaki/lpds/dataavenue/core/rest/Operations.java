package hu.sztaki.lpds.dataavenue.core.rest;

import hu.sztaki.lpds.dataavenue.core.AdaptorRegistry;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status;

@Path("/operations")
public class Operations {

	private static final Logger log = LoggerFactory.getLogger(Authentication.class); 
	
	private void logRequest(final String method, final HttpServletRequest request) {
		log.info("REST " + method + ", from: " + request.getRemoteAddr());
	}
	
	@GET
	@Path("{protocol}")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response list(
			@Context HttpServletRequest request,
			@PathParam("protocol") String protocol) {
		try {
		logRequest("operations", request);
    	
	    Adaptor adaptor;
	    try { adaptor = AdaptorRegistry.getAdaptorInstance(protocol); }
	    catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
		
        JSONArray jsonArray = new JSONArray();
    	for (OperationsEnum i: adaptor.getSupportedOperationTypes(protocol)) jsonArray.put(i.name());
    	
    	log.debug(jsonArray.toString(4));
		
		return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonArray.toString()).build();

		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
}