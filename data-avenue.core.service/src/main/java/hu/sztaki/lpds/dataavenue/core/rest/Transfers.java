package hu.sztaki.lpds.dataavenue.core.rest;

import hu.sztaki.lpds.dataavenue.core.AdaptorRegistry;
import hu.sztaki.lpds.dataavenue.core.ClientAuthentication;
import hu.sztaki.lpds.dataavenue.core.CopyTaskManager;
import hu.sztaki.lpds.dataavenue.core.ExtendedTransferMonitor;
import hu.sztaki.lpds.dataavenue.core.TaskManager;
import hu.sztaki.lpds.dataavenue.core.TransferDetails;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.AsyncCommands;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

import java.util.Collection;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status;

@Path("/transfers")
public class Transfers {
	private static final Logger log = LoggerFactory.getLogger(Transfers.class); 
	
	private void logRequest(final String method, final HttpHeaders httpHeaders, final HttpServletRequest request) {
		String key = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_KEY).get(0) : null;
		String uri = httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI) != null ? httpHeaders.getRequestHeader(CustomHttpHeaders.HTTP_HEADER_URI).get(0) : null;
		log.info("REST " + method + ", key: " + key + ", URI: " + uri + ", from: " + request.getRemoteAddr());
	}

	@GET 
	@Path("{id}")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response getStatus (
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			@PathParam("id") String transferId) {
		logRequest("GET", headers, request);
		try {
			@SuppressWarnings("unused")
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	
			TransferDetails status;
			try { status = TaskManager.getInstance().getTaskDetails(transferId); }
			catch (TaskIdException e) { return Response.status(Status.NOT_FOUND).entity("Invalid transfer id: " + transferId).build(); }
				
	        JSONObject jsonObject = new JSONObject();
	        jsonObject.put("source", status.getSource());
	        jsonObject.put("target", status.getTarget());
		        
	        jsonObject.put("bytesTransferred", status.getBytesTransferred());
	        jsonObject.put("size", status.getTotalDataSize());
	        jsonObject.put("status", status.getState());
	        if (status.getFailureCause() != null) jsonObject.put("failure", status.getFailureCause());
		        
	        jsonObject.put("started", status.getStarted());
	        jsonObject.put("ended", status.getEnded());
	        jsonObject.put("serverTime", status.getNow());
		        
			return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonObject.toString()).build();
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}

	@GET 
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response getAllStatus (
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@Context HttpHeaders headers,
			@Context HttpServletRequest request) {
		logRequest("GET", headers, request);
		try {
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	
			long now = System.currentTimeMillis();
			
			Collection<ExtendedTransferMonitor> tasks = TaskManager.getInstance().getAllNonAcknowledgedUserTranferDetails(key);
			JSONArray jsonArray = new JSONArray(); 
			for (ExtendedTransferMonitor status: tasks) {
		        JSONObject jsonObject = new JSONObject();
		        jsonObject.put("id", status.getTaskId()); 
		        jsonObject.put("source", status.getSource());
		        jsonObject.put("target", status.getDestination());
			        
		        jsonObject.put("bytesTransferred", status.getBytesTransferred());
		        jsonObject.put("size", status.getTotalDataSize());
		        jsonObject.put("status", status.getState());
		        if (status.getFailureCause() != null) jsonObject.put("failure", status.getFailureCause());
			        
		        jsonObject.put("started", status.getStarted());
		        jsonObject.put("ended", status.getEnded());
		        jsonObject.put("serverTime", now);
		        jsonArray.put(jsonObject);
			}
			return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonArray.toString()).build();
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
	
	@DELETE 
	@Path("{id}")
	@Produces(MediaType.TEXT_PLAIN)
	public Response cancel (
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			@PathParam("id") String transferId) {
		logRequest("DELETE", headers, request);
		try {
			@SuppressWarnings("unused")
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
			
			try { TaskManager.getInstance().cancelTransfer(transferId); } 
			catch (TaskIdException e) { return Response.status(Status.NOT_FOUND).entity("Invalid transfer id: " + transferId).build(); } 
			catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			
			return Response.status(Status.OK).build();

		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}

	@PUT 
	@Path("{id}")
	// set transfer acknoledged by the user
	public Response acknowledge (
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			@PathParam("id") String transferId) {
		logRequest("PUT", headers, request);
		try {
			@SuppressWarnings("unused")
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
			
			try { TaskManager.getInstance().acknowledgeTransfer(transferId); } 
			catch (TaskIdException e) { return Response.status(Status.NOT_FOUND).entity("Invalid transfer id: " + transferId).build(); } 
			catch (OperationException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			
			return Response.status(Status.OK).build();

		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
	
	@POST 
	@Produces(MediaType.TEXT_PLAIN)
	// start a new transfer
	public Response copy (
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_KEY) String authorization,  
			@HeaderParam(CustomHttpHeaders.HTTP_HEADER_URI) String xuri, 
			@Context HttpHeaders headers,
			@Context HttpServletRequest request,
			String entityBody) {
		logRequest("POST", headers, request);
		try {
			String key;
			try { key = ClientAuthentication.getAuthenticatedId(authorization, request); } 
			catch (Exception e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
	
			DefaultURIBaseImpl sourceUri;
			try { sourceUri = new DefaultURIBaseImpl(xuri); }
			catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			
			Credentials sourceCredentials;
			sourceCredentials = CredentialsUtils.createCredentialsFromHttpHeader(headers);
	
	        Adaptor sourceAdaptor;
	        try { sourceAdaptor = AdaptorRegistry.getAdaptorInstance(sourceUri.getProtocol()); }
	        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	
	        JSONObject requestBody = null;
	        if (entityBody != null && entityBody.length() > 0) {
	    		try { requestBody = new JSONObject(new JSONTokener(entityBody)); }
	    		catch (JSONException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        } else { return Response.status(Status.BAD_REQUEST).entity("Missing entity body!").build(); }
	        
			DefaultURIBaseImpl targetUri;
			try { targetUri = new DefaultURIBaseImpl(requestBody.getString("target")); }
	        catch (JSONException e) { return Response.status(Status.BAD_REQUEST).entity("No target!").build(); }
			catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
	        Adaptor targetAdaptor;
	        try { targetAdaptor = AdaptorRegistry.getAdaptorInstance(targetUri.getProtocol()); }
	        catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	        
	        boolean overwrite = false;
	        try { overwrite = requestBody.getBoolean("overwrite"); } 
	        catch (JSONException e) {} // do not overwrite if missing
	
	        boolean copy = true;
	        try { copy = !requestBody.getBoolean("move"); } 
	        catch (JSONException e) {} // do not move if missing
	
	        Credentials targetCredentials = null;
	        try { 
	        	targetCredentials = CredentialsUtils.createCredentialsFromJSON(requestBody.getJSONObject("credentials"));
	        } 
	        catch (JSONException e) { log.debug("No target credentials"); }
	        
	    	// if target file name is not given on file copy, auto-add it
	    	if (sourceUri.isFile() && targetUri.isDir()) targetUri = new DefaultURIBaseImpl(targetUri.getURI() + sourceUri.getEntryName()); 
	    	if (sourceUri.isDir() && targetUri.isFile()) throw new OperationException("Cannot copy a directory to a file!");
	    	if (sourceUri.isIdenticalFileOrDirEntry(targetUri) || sourceUri.isSameSubdirWithoutFileName(targetUri) || targetUri.isSubdirOf(sourceUri)) {
	    		log.warn("Copy/move entry to itself {} -> {} ", sourceUri, targetUri);
	    		throw new OperationException("Cannot a file or copy a directory into itself!");
	    	}
	        
	        AsyncCommands managingAdaptor;
	    	// if source and target adaptors are identical and copy operation is supported, use its internal function (possibly, third-party transfer)
	    	if (	
	    			(sourceAdaptor == targetAdaptor && sourceUri.isDir() && copy && sourceAdaptor.getSupportedOperationTypes(sourceUri, targetUri).contains(OperationsEnum.COPY_DIR)) ||
	    			(sourceAdaptor == targetAdaptor && sourceUri.isDir() && !copy && sourceAdaptor.getSupportedOperationTypes(sourceUri, targetUri).contains(OperationsEnum.MOVE_DIR)) ||
	    			(sourceAdaptor == targetAdaptor && sourceUri.isFile() && copy &&  sourceAdaptor.getSupportedOperationTypes(sourceUri, targetUri).contains(OperationsEnum.COPY_FILE)) ||
	    			(sourceAdaptor == targetAdaptor && sourceUri.isFile() && !copy && sourceAdaptor.getSupportedOperationTypes(sourceUri, targetUri).contains(OperationsEnum.MOVE_FILE))) {
	    		log.debug("Using adator's supplied copy/move function...");
	    		managingAdaptor = sourceAdaptor;
	    	} else {
	    		log.debug("Using core streaming copy/move function...");
	    		managingAdaptor = CopyTaskManager.getInstance();
	    	}
	    		
	       	// create a new monitor instance (some data are managed by the adaptor)
	       	ExtendedTransferMonitor monitor =
	       			sourceUri.isDir() ? 
	       						new ExtendedTransferMonitor(key, sourceUri.getURI(), targetUri.getURI(), copy ? OperationsEnum.COPY_DIR : OperationsEnum.MOVE_DIR) :
	       						new ExtendedTransferMonitor(key, sourceUri.getURI(), targetUri.getURI(), copy ? OperationsEnum.COPY_FILE : OperationsEnum.MOVE_FILE);
	       						
	       	// start internal task, provide the monitor to it
	       	String adaptorManagedTaskId;
	       	try {
	       		adaptorManagedTaskId =
	   				copy ?
	    				managingAdaptor.copy(sourceUri, sourceCredentials, targetUri, targetCredentials, overwrite, monitor) :
	    				managingAdaptor.move(sourceUri, sourceCredentials, targetUri, targetCredentials, overwrite, monitor);
	       	} 
			catch (URIException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
	       	catch (CredentialException e) { return Response.status(Status.UNAUTHORIZED).entity(e.getMessage()).build(); }
			catch (OperationException e) { return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	
	       	// set monitor's internal task id (potential cancel)
	   		monitor.setManagingAdaptor(managingAdaptor);
	   		monitor.setInternalTaskId(adaptorManagedTaskId);		
	       	// if no exception, register this task in taskregistry
	   		TaskManager.getInstance().registerTransferMonitor(monitor);
	        
	        String transferId = monitor.getTaskId();
	        
			return Response.status(Status.OK).entity(transferId).build();

		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
}