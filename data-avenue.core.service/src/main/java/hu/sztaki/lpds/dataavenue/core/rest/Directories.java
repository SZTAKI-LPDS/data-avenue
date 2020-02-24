package hu.sztaki.lpds.dataavenue.core.rest;

import java.util.List;

import hu.sztaki.lpds.dataavenue.core.AdaptorRegistry;
import hu.sztaki.lpds.dataavenue.core.ClientAuthentication;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase.URIType;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/{parameter: directory|directories}")
public class Directories {
	
	private static final Logger log = LoggerFactory.getLogger(Directories.class); 
	
	private void logRequest(final String method, final HttpHeaders httpHeaders, final HttpServletRequest request) {
		String key = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY).get(0) : null;
		String uri = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI).get(0) : null;
		log.info("" + method + ": " + uri + ", key: " + key +  ", from: " + request.getRemoteAddr());
	}
	
	@GET
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response list(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request) {
		try {
			logRequest("GET", headers, request);
			
			@SuppressWarnings("unused")
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
			
			URIBase uri;
			try { uri = new DefaultURIBaseImpl(xuri); }
			catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			if (uri.getType() != URIBase.URIType.DIRECTORY && uri.getType() != URIBase.URIType.URL) { return Response.status(Status.BAD_REQUEST).entity("Not a directory URI!").build(); }
			
			Credentials credentials;
			credentials = CredentialsUtils.createCredentialsFromHttpHeader(headers);
	
	        Adaptor adaptor;
	        try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); }
	        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	
	        DataAvenueSession session = HttpSessionUtils.getSession(headers, request);
	        
	        List <URIBase> dirContents;
	        try { dirContents = adaptor.list(uri, credentials, session); }
	        catch (URIException e) { return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build(); }
	        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
	        JSONArray jsonArray = new JSONArray();
	    	for (URIBase i : dirContents) {
	    		jsonArray.put(i.getEntryName() + (URIType.DIRECTORY == i.getType() ? "/" : ""));
	    	}
			
	    	log.info("OK (" + jsonArray.length() + " items)");
			return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonArray.toString()).build();
			
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
	
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public Response mkdir(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			String body) {
		try {
			logRequest("POST", headers, request);
	    	
			@SuppressWarnings("unused")
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	
			URIBase uri;
			try { uri = new DefaultURIBaseImpl(xuri); }
			catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			if (uri.getType() != URIBase.URIType.DIRECTORY) { return Response.status(Status.BAD_REQUEST).entity("Not a directory URI!").build(); }
			
			Credentials credentials;
			credentials = CredentialsUtils.createCredentialsFromHttpHeader(headers);
	
	        Adaptor adaptor;
	        try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); }
	        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
	        DataAvenueSession session = HttpSessionUtils.getSession(headers, request);
	
	        try { adaptor.mkdir(uri, credentials, session); }
	        catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
	    	log.info("OK (directory created)");
			return Response.status(Status.OK).build();
			
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}

	@PUT
	@Produces(MediaType.TEXT_PLAIN)
	public Response rename() { 
		return Response.status(Status.METHOD_NOT_ALLOWED).entity("Operation not supported!").build();
	}
	
	@DELETE
	@Produces(MediaType.TEXT_PLAIN)
	public Response rmdir(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			String body) {
		try {
			logRequest("DELETE", headers, request);
	    	
			@SuppressWarnings("unused")
			String key;
			try { ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	
			URIBase uri;
			try { uri = new DefaultURIBaseImpl(xuri); }
			catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			if (uri.getType() != URIBase.URIType.DIRECTORY) { return Response.status(Status.BAD_REQUEST).entity("Not a directory URI!").build(); }
			
			Credentials credentials;
			credentials = CredentialsUtils.createCredentialsFromHttpHeader(headers);
	
	        Adaptor adaptor;
	        try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); }
	        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
	        DataAvenueSession session = HttpSessionUtils.getSession(headers, request);
	        
	        try { adaptor.rmdir(uri, credentials, session); }
	        catch (URIException e) { return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build(); }
	        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }

	    	log.info("OK (directory deleted)");
			return Response.status(Status.OK).build();
		
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
}