package hu.sztaki.lpds.dataavenue.core.rest;

import hu.sztaki.lpds.dataavenue.core.AdaptorRegistry;
import hu.sztaki.lpds.dataavenue.core.ClientAuthentication;
import hu.sztaki.lpds.dataavenue.core.HttpAlias;
import hu.sztaki.lpds.dataavenue.core.HttpAliasRegistry;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.core.interfaces.impl.DirEntryImpl;
import hu.sztaki.lpds.dataavenue.core.interfaces.impl.FileEntryImpl;
import hu.sztaki.lpds.dataavenue.core.interfaces.impl.URIFactory;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.DirectURLsSupported;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/aliases") 
@Deprecated
public class Aliases {

	private static final Logger log = LoggerFactory.getLogger(Aliases.class); 

	public static final String ALIASES_PREFIX = "/aliases";

	private void logRequest(final String method, final HttpHeaders httpHeaders, final HttpServletRequest request) {
		String key = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY).get(0) : null;
		String uri = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI).get(0) : null;
		log.info("REST " + method + ", key: " + key + ", URI: " + uri + ", from: " + request.getRemoteAddr());
	}
	
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response create(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			String entityBody) {
		
		DataAvenueSession session = null;
		try {
			logRequest("POST", headers, request);
	
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        
			URIBase uri;
			try { uri = URIFactory.createURI(xuri); }
			catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			
			Credentials credentials;
			credentials = CredentialsUtils.createCredentialsFromHttpHeader(headers);
	
	        Adaptor adaptor;
	        try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); }
	        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	
	        session = HttpSessionUtils.getSession(headers, request);
//	        if (session == null) return Response.status(Status.UNAUTHORIZED).entity("Session expired!").build();;
	        
	        JSONObject options = new JSONObject();
	        if (entityBody != null && entityBody.length() > 0) {
	    		log.debug(entityBody);
	    		try { options = new JSONObject(new JSONTokener(entityBody)); }
	    		catch (JSONException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	    		log.debug(options.toString(4));
	        }
			
	        int lifetime = 3600;
	        try { if (options.get("lifetime") instanceof Integer) lifetime = (Integer) options.get("lifetime"); }
	        catch (JSONException e) {} // no such key or invalid
	        
	        boolean read = true;
	        try { if (options.get("read") instanceof Boolean) read = (Boolean) options.get("read"); }
	        catch (JSONException e) {} 
	        
	        boolean archive = false;
	        try { if (options.get("archive") instanceof Boolean) archive = (Boolean) options.get("archive"); }
	        catch (JSONException e) {} 
	        
	        boolean redirect = true;
	        try { if (options.get("redirect") instanceof Boolean) redirect = (Boolean) options.get("redirect"); }
	        catch (JSONException e) {}  
	        
			if (!archive && !(uri instanceof FileEntryImpl)) { return Response.status(Status.BAD_REQUEST).entity("Not a file URI!").build(); }
			else if (archive && !(uri instanceof DirEntryImpl)) { return Response.status(Status.BAD_REQUEST).entity("Not a directory URI (archive)!").build(); }
			
	    	String directURL = null;
			if (redirect && adaptor instanceof DirectURLsSupported) {
		    	try { directURL = ((DirectURLsSupported) adaptor).createDirectURL(uri, credentials, read, lifetime, null); }
		    	catch (Exception e) { log.warn("Cannot create direct URL!", e); }
			}
	    	
	        String aliasId = null;
	        try { aliasId = HttpAliasRegistry.getInstance().createAndRegisterHttpAlias(key, uri, credentials, session, read, lifetime, archive, directURL); } 
	        catch (URIException e) { return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build(); }
	        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        catch (NotSupportedProtocolException e) {return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();	} 
	        
	        String requestURL = request.getRequestURL().toString();
	        requestURL = requestURL.substring(0, requestURL.indexOf("/rest/aliases")); // cut /rest/aliases suffix
	
	        JSONObject jsonObject = new JSONObject();
	        jsonObject.put("id", aliasId);
	        jsonObject.put("url", requestURL  + ALIASES_PREFIX + "/" +  aliasId);
	        if (directURL != null) jsonObject.put("directURL", directURL);
	    	log.debug(jsonObject.toString(4));
			
			return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonObject.toString()).build();
			
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
//		finally { HttpSessionUtils.discardSession(headers, request, session); } 
	}
	
	@DELETE
	@Path("{id}")
	@Produces(MediaType.TEXT_PLAIN)
	public Response delete(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			@PathParam("id") String id) {
		
		try {
			logRequest("DELETE", headers, request);
	
			@SuppressWarnings("unused")
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        
	        try { 
	        	HttpAlias alias = HttpAliasRegistry.getInstance().deleteHttpAlias(id);
	        	if (alias == null) return Response.status(Status.NOT_FOUND).entity("Invalid alias id!").build();
	        } 
	        catch (Exception e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
			return Response.status(Status.OK).build();

		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
}