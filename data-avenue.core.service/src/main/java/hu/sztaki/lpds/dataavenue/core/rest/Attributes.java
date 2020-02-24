package hu.sztaki.lpds.dataavenue.core.rest;

import java.util.List;
import java.util.Vector;

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
import javax.ws.rs.Consumes;
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
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/attributes")
public class Attributes {
	private static final Logger log = LoggerFactory.getLogger(Attributes.class);
	
	private static final String NAME = "name", DATE = "date", SIZE = "size", PERM = "perm", /*OWNER = "owner",*/ OTHER = "other" /*, UNIT = "unit"*/; 
	
	private void logRequest(final String method, final HttpHeaders httpHeaders, final HttpServletRequest request) {
		String key = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY).get(0) : null;
		String uri = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI).get(0) : null;
		log.info("REST " + method + ", key: " + key + ", URI: " + uri + ", from: " + request.getRemoteAddr());
	}
	
	/*
	 * Return attibutes of a single file or directory
	 */
	@GET
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response getAttributes(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request) {
		
		DataAvenueSession session = null;
		try {
			logRequest("GET", headers, request);
			
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
	
	        session = HttpSessionUtils.getSession(headers, request);
	        
	        URIBase uriDetails = null;
	        try { uriDetails = adaptor.attributes(uri, credentials, session); } 
	        catch (URIException e) { return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build(); }
	        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	
	        JSONObject jsonObject = new JSONObject();
	        jsonObject.put(NAME, uriDetails.getEntryName() + (uriDetails.getType() == URIType.DIRECTORY ? URIBase.PATH_SEPARATOR : ""));
	        jsonObject.put(DATE, uriDetails.getLastModified());
	        jsonObject.put(SIZE, uriDetails.getSize());
//	        jsonObject.put(UNIT, uriDetails.getSizeUnit());
	        jsonObject.put(PERM, uriDetails.getPermissions());
	        if (uriDetails.getDetails() != null) jsonObject.put(OTHER, uriDetails.getDetails());
			
			return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonObject.toString()).build();
			
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
	
	/*
	 * Modify attributes of a file or directory.
	 */
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response modifyAttributes(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			String entityBody) {
		 return Response.status(Status.BAD_REQUEST).entity("Not yet implemented").build();
	}

	
	/*
	 * Return attibutes of a multiple files or subdirectories with parent xuri/
	 * If no subentries specified (null or empty), all subentries are returned with details.
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response getSubentriesAttributesSet(
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			String entityBody) {

		DataAvenueSession session = null;
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
	
	        Adaptor adaptor;
	        try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); }
	        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
	        session = HttpSessionUtils.getSession(headers, request);
	        
	        JSONArray entriesJSON = new JSONArray();
	        if (entityBody != null && entityBody.length() > 0) {
	    		log.debug(entityBody);
	    		try { entriesJSON = new JSONArray(new JSONTokener(entityBody)); }
	    		catch (JSONException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	    		log.debug(entriesJSON.toString(4));
	        } else { 
	        	log.debug("Getting attributes of all subentries in " + uri.getURI());
	        }
	        
	        List<String> entries = new Vector<String>();
	        for (int i = 0; i < entriesJSON.length(); i++) {
	        	Object object = entriesJSON.get(i);
	        	if (!(object instanceof String)) continue;
	        	entries.add((String) object);
	        }
	        
	        List<URIBase> uriDetailsList = null;
	        try { uriDetailsList = adaptor.attributes(uri, credentials, session, entries); } 
	        catch (URIException e) { return Response.status(Status.NOT_FOUND).entity(e.getMessage()).build(); }
	        catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	        catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	
	        JSONArray jsonArray = new JSONArray();
	
	        for (URIBase uriDetails: uriDetailsList) {
	            JSONObject jsonObject = new JSONObject();
	            jsonObject.put(NAME, uriDetails.getEntryName() + (uriDetails.getType() == URIType.DIRECTORY ? URIBase.PATH_SEPARATOR : ""));
	            jsonObject.put(DATE, uriDetails.getLastModified());
	            jsonObject.put(SIZE, uriDetails.getSize());
//	            jsonObject.put(UNIT, uriDetails.getSizeUnit());
	            jsonObject.put(PERM, uriDetails.getPermissions());
		        if (uriDetails.getDetails() != null) jsonObject.put(OTHER, uriDetails.getDetails());
	            jsonArray.put(jsonObject);
	        }
	        
			return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonArray.toString()).build();

		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
}