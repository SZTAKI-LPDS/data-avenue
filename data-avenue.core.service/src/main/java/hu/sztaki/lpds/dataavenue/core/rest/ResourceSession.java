package hu.sztaki.lpds.dataavenue.core.rest;

import java.io.InputStream;
import java.net.URI;
import java.util.UUID;

import hu.sztaki.lpds.dataavenue.core.AdaptorRegistry;
import hu.sztaki.lpds.dataavenue.core.ClientAuthentication;
import hu.sztaki.lpds.dataavenue.core.Statistics;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.DirectURLsSupported;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DataAvenueSessionImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status;

@Path("/resourcesession") 
public class ResourceSession {

	private static final Logger log = LoggerFactory.getLogger(ResourceSession.class); 

	private void logRequest(final String method, final HttpHeaders httpHeaders, final HttpServletRequest request) {
		String key = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY).get(0) : null;
		String uri = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI).get(0) : null;
		log.info("REST " + method + ", key: " + key + ", URI: " + uri + ", from: " + request.getRemoteAddr());
	}
	
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public Response create(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_REDIRECT) String xredirect,
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			String entityBody) {
		try {
			logRequest("POST", headers, request);
	
			@SuppressWarnings("unused")
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        
			DefaultURIBaseImpl uri;
			try { uri = new DefaultURIBaseImpl(xuri); }
			catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			
			Credentials credentials;
			credentials = CredentialsUtils.createCredentialsFromHttpHeader(headers);
	
	        String sessionId = UUID.randomUUID().toString(); 
	        HttpSession httpSession = request.getSession(false); // get existing session or null
        	if (httpSession == null) { // no session yet
        		log.debug("Creating new HTTP session...");
        		httpSession = request.getSession(true); // create a new session
        	} else {
        		if (request.isRequestedSessionIdValid()) {
            		log.debug("Using existing HTTP session...");
        		} else {
        			log.debug("Session expired! Creating new HTTP session.");
        			httpSession = request.getSession(true); // create a new session
        		}
        	}

            // redirect
            boolean redirect = xredirect != null && !"no".equals(xredirect);
        	httpSession.setAttribute(sessionId, new SessionData(uri, credentials, redirect));
        	log.debug("HTTP session id: " + httpSession.getId());
	        log.debug("Internal session id: " + sessionId);
        	
			return Response.status(Status.OK).type(MediaType.TEXT_PLAIN).entity(sessionId).build();
			
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
	
	private class SessionData {
		private final URIBase uri;
		private final Credentials credentials;
		private boolean redirect;
		public boolean isRedirect() {
			return redirect;
		}
		public Credentials getCredentials() {
			return credentials;
		}
		public URIBase getUri() {
			return uri;
		}
		SessionData(URIBase uri, Credentials credentials,  boolean redirect) {
			this.uri = uri;
			this.credentials = credentials;
			this.redirect = redirect;
		}
	}
	
	@GET
	@Path("{id}")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_OCTET_STREAM})
	public Response download (
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			@PathParam("id") String id) {
		
		try {
			logRequest("GET", headers, request);
			
			HttpSession httpSession = request.getSession(false); // get existing session or null
        	if (httpSession == null) Response.status(Status.BAD_REQUEST).entity("Session id not found: " + id).build(); 
        	if (!request.isRequestedSessionIdValid()) Response.status(Status.BAD_REQUEST).entity("Session expired: " + id).build(); 
			SessionData sessionData = (SessionData) httpSession.getAttribute(id);
			
        	log.debug("HTTP session id: " + httpSession.getId());
	        log.debug("Internal session id: " + id);
			
			if (sessionData == null) Response.status(Status.BAD_REQUEST).entity("Session data not found with id: " + id).build();
			URIBase uri = sessionData.getUri();
			Credentials credentials = sessionData.getCredentials();
			boolean redirect = sessionData.isRedirect();
			
			Adaptor adaptor;
			try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); }
			catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }

			// this part copied form File.download()
	        if (redirect) {
	        	log.debug("Trying to create redirect URL");
	    		if (redirect && adaptor instanceof DirectURLsSupported) {
	    	    	try { 
	    	    		String redirectURL = ((DirectURLsSupported) adaptor).createDirectURL(uri, credentials, true, 600, null);
	    	    		log.debug("Redirect URL: " + redirectURL);
	    	    		URI location = UriBuilder.fromUri(redirectURL).build();
	    	    		log.debug("Redirecting client");
	    				httpSession.removeAttribute(id);
	    	    		return Response.temporaryRedirect(location).build();
	    	    	}
	    	    	catch (Exception e) { log.warn("Cannot create direct URL!", e); }
	    		}
	        }
	        
	        InputStream in = null;
	        try {
	        	
	        	log.debug("Getting file size of " + uri.toString());
	        	DataAvenueSession session = new DataAvenueSessionImpl();
	        	long size = -1l;
	        	try { size = adaptor.getFileSize(uri, credentials, session); }
	        	catch (Exception e) { log.debug("Cannnot determine file size " + e.getClass().getName()); }
	        	if (size > 0) Statistics.incBytesTransferred(size);

	        	log.debug("Getting input stream of " + uri.toString());
	        	in = adaptor.getInputStream(uri, credentials, session);
	        	
	        	ResponseBuilder rb = Response.status(Status.OK).header("Content-Disposition", "attachment; filename=\"" + uri.getEntryName() + "\"" );
	        	if (size >= 0l) rb.header("Content-Length", Long.toString(size));

	        	log.debug("Transfer started"); 
	        	return rb.entity(in).type(MediaType.APPLICATION_OCTET_STREAM).build();
	        }
	        catch (URIException e) { return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build(); }
	        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        finally { 
	        	// do not close input stream, or discard session (closing in causes pipe closed exception) 
				httpSession.removeAttribute(id);
	        }
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
}
