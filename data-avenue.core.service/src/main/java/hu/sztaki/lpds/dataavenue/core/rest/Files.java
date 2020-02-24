package hu.sztaki.lpds.dataavenue.core.rest;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

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
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationNotSupportedException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriBuilder;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status;

@Path("/{parameter: file|files}")
public class Files {
	private static final Logger log = LoggerFactory.getLogger(Files.class); 

	protected static final int DEFAULT_BUFFER_SIZE = 16 * 1024; // 16k

	private void logRequest(final String method, final HttpHeaders httpHeaders, final HttpServletRequest request) {
		String key = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY).get(0) : null;
		String uri = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI).get(0) : null;
		log.info("" + method + ": " + uri + ", key: " + key +  ", from: " + request.getRemoteAddr());
	}

	@GET 
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_OCTET_STREAM})
	public Response download (
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri,
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_REDIRECT) String xredirect,
			@Context HttpHeaders headers,
			@Context HttpServletRequest request) {
		try {
		logRequest("GET", headers, request);
    	
		@SuppressWarnings("unused")
		String key;
		try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
		catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }

		DefaultURIBaseImpl uri;
		try { uri = new DefaultURIBaseImpl(xuri); }
		catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
		if (!uri.isFile()) { return Response.status(Status.BAD_REQUEST).entity("Not a file URI!").build(); }
		
		Credentials credentials;
		credentials = CredentialsUtils.createCredentialsFromHttpHeader(headers);

        Adaptor adaptor;
        try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); }
        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }

        DataAvenueSession session = HttpSessionUtils.getSession(headers, request);

        // redirect
        boolean redirect = xredirect != null && !"no".equals(xredirect);
        if (redirect) {
        	log.debug("Trying to create redirect URL");
    		if (redirect && adaptor instanceof DirectURLsSupported) {
    	    	try { 
    	    		String redirectURL = ((DirectURLsSupported) adaptor).createDirectURL(uri, credentials, true, 600, null);
    	    		log.debug("Redirect URL: " + redirectURL);
    	    		URI location = UriBuilder.fromUri(redirectURL).build();
    	    		log.debug("Redirecting client");
    	    		return Response.temporaryRedirect(location).build();
    	    	}
    	    	catch (Exception e) { log.warn("Cannot create direct URL!", e); }
    		}
        }
        
        InputStream in = null;
        try {
        	in = adaptor.getInputStream(uri, credentials, session);
        	
        	long size = 0l;
        	try { size = adaptor.getFileSize(uri, credentials, session); }
        	catch (Exception e) { log.debug("Cannnot determine file size", e); }
        	Statistics.incBytesTransferred(size);
        	
        	ResponseBuilder rb = Response.status(Status.OK).header("Content-Disposition", "attachment; filename=\"" + uri.getEntryName() + "\"" );
        	if (size > 0l) rb.header("Content-Length", Long.toString(size));
        	return rb.entity(in).type(MediaType.APPLICATION_OCTET_STREAM).build();
        }
        catch (URIException e) { return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build(); }
        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
        finally { 
        	// do not close input stream, or discard session 
        	// (closing in causes pipe closed exception) 
        	log.debug("Transfer started"); 
        }
		} catch (Throwable e) { e.printStackTrace(); log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
	
	/*
	 * NOTE: On redirect, Ceph S3 storage may return AccessDenied if Content-Type header is not specified (either application/octet-stream, binary/octet-stram)
	 * To avoid this specify header -H "Content-Type: " (optionally:  -H "Expect: 100-continue") or use curl --upload-file filename
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public Response uploadPOST ( // do not overwrite
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri,
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_REDIRECT) String xredirect,
			@HeaderParam("Content-Length") String contentLengthHeader,
			@Context HttpHeaders headers,
			@Context HttpServletRequest request) {
		logRequest("POST", headers, request);
		return upload(authorization, xuri, xredirect, contentLengthHeader, headers, request, false);
	}
	
	@PUT 
	@Produces(MediaType.TEXT_PLAIN)
	public Response uploadPUT ( // overwrite
		@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
		@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri,
		@HeaderParam(CustomHttpHeaders.HTTP_HEADER_REDIRECT) String xredirect,
		@HeaderParam("Content-Length") String contentLengthHeader,
		@Context HttpHeaders headers,
		@Context HttpServletRequest request) {
		logRequest("PUT", headers, request);
		return upload(authorization, xuri, xredirect, contentLengthHeader, headers, request, true);
	}
	
	private Response upload ( 
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri,
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_REDIRECT) String xredirect,
			@HeaderParam("Content-Length") String contentLengthHeader,
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			boolean overwrite) {
			try {
				@SuppressWarnings("unused")
				String key;
				try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
				catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
		
				DefaultURIBaseImpl uri;
				try { uri = new DefaultURIBaseImpl(xuri); }
				catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
				if (!uri.isFile()) { return Response.status(Status.BAD_REQUEST).entity("Not a file URI!").build(); }
				
				Credentials credentials;
				credentials = CredentialsUtils.createCredentialsFromHttpHeader(headers);
		
		        Adaptor adaptor;
		        try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); }
		        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
		
		        DataAvenueSession session = HttpSessionUtils.getSession(headers, request);
		        
		        if (!overwrite && adaptor.isReadable(uri, credentials, session)) {
		        	return Response.status(Status.BAD_REQUEST).entity("URI already exists (use PUT to overwrite)!").build();
		        }
		        
		        // TODO delete if exists (s3 overwrites by default, no need to delete first)
		        
		        // redirect
		        boolean redirect = xredirect != null && !"no".equals(xredirect);
		        if (redirect) {
		    		if (redirect && adaptor instanceof DirectURLsSupported) {
		    	    	try { 
		    	    		String redirectURL = ((DirectURLsSupported) adaptor).createDirectURL(uri, credentials, false, 600, null);
		    	    		URI location = UriBuilder.fromUri(redirectURL).build();
		    	    		log.debug("Redirecting client to URL: " + redirectURL);
		    	    		return Response.temporaryRedirect(location).build();
		    	    	}
		    	    	catch (Exception e) { log.warn("Cannot create direct URL!", e); }
		    		}
		        }
		        
				long contentLength = -1;
				try { contentLength = Long.parseLong(contentLengthHeader); } catch (NumberFormatException x) {};
				
				try { doWriteInputStreamToOutputStream(request, adaptor, uri, credentials, session, contentLength);	}
		        catch (URIException e) { return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build(); }
		        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
		        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
		        catch (IOException e) { return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
			
		    	log.info("OK (file uploaded: " + xuri + ")");

		        return Response.status(Status.OK).build();
		        
			} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
	
	private void doWriteInputStreamToOutputStream(HttpServletRequest request, Adaptor adaptor, URIBase uri, Credentials credentials, DataAvenueSession session, long contentLength) throws URIException, CredentialException, OperationException, IOException {
        InputStream in = null;
        OutputStream out = null;
		try {
	       	in = request.getInputStream();
	        	
	       	// try to write from input stream
	       	boolean writeThroughStreamSuccessful = false;
	       	try { 
	       		adaptor.writeFromInputStream(uri, credentials, session, in, contentLength);
	       		writeThroughStreamSuccessful = true;
	           	Statistics.incBytesTransferred(contentLength);  
	       	} catch (OperationNotSupportedException x) {} // size out of range, skip
	        	
	       	if (!writeThroughStreamSuccessful) {
	        	out = adaptor.getOutputStream(uri, credentials, session, contentLength);
	            byte [] buffer = new byte[DEFAULT_BUFFER_SIZE];
		        int readBytes;
		        long totalBytes = 0l;
		        while ((readBytes = in.read(buffer)) > 0) { 
		        	out.write(buffer, 0, readBytes); 
		        	totalBytes += readBytes;
		        }
	        	Statistics.incBytesTransferred(totalBytes);  
	       	}
		} finally {
	        	if (in != null) try { in.close(); } catch(IOException e) {}
	        	if (out != null) try { out.close(); } catch(IOException e) { throw e; }
		}
	}
        
	@PUT	
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)
	public Response rename(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			String entityBody) {
		try {
			@SuppressWarnings("unused")
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
			
			DefaultURIBaseImpl uri;
			try { uri = new DefaultURIBaseImpl(xuri); }
			catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			
			Credentials credentials;
			credentials = CredentialsUtils.createCredentialsFromHttpHeader(headers);
	
	        Adaptor adaptor;
	        try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); }
	        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	
	        JSONObject requestBody = null;
	        if (entityBody != null && entityBody.length() > 0) {
	    		try { requestBody = new JSONObject(new JSONTokener(entityBody)); }
	    		catch (JSONException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        } else { return Response.status(Status.BAD_REQUEST).entity("Missing entity body!").build(); }
	        
	        String newName = "newname";
	        try { newName = requestBody.getString("newName"); } 
	        catch (JSONException e) {
	        	throw new OperationException("JSON object containing newName field expected");
	        } 
	        DataAvenueSession session = HttpSessionUtils.getSession(headers, request);
	        
	        try { adaptor.rename(uri, newName, credentials, session); }
	        catch (URIException e) { return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build(); }
	        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
	    	log.info("OK (file renamed: " + newName + ")");

			return Response.status(Status.OK).build();

		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
	
	@DELETE
	@Produces(MediaType.TEXT_PLAIN)
	public Response delete (
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request) {
		try {
			logRequest("DELETE", headers, request);
	    	
			@SuppressWarnings("unused")
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	
			DefaultURIBaseImpl uri;
			try { uri = new DefaultURIBaseImpl(xuri); }
			catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			if (uri.getType() != URIBase.URIType.FILE) { return Response.status(Status.BAD_REQUEST).entity("Not a file URI!").build(); }
			
			Credentials credentials;
			credentials = CredentialsUtils.createCredentialsFromHttpHeader(headers);
	
	        Adaptor adaptor;
	        try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); }
	        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
	        DataAvenueSession session = HttpSessionUtils.getSession(headers, request);
	        
	        try { adaptor.delete(uri, credentials, session); }
	        catch (URIException e) { return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build(); }
	        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
	    	log.info("OK (file deleted: " + xuri + ")");

			return Response.status(Status.OK).build();
		
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}	
}