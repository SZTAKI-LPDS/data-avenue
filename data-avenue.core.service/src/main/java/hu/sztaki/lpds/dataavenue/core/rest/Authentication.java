package hu.sztaki.lpds.dataavenue.core.rest;

import hu.sztaki.lpds.dataavenue.core.AdaptorRegistry;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationField;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationType;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationTypeList;
import hu.sztaki.lpds.dataavenue.interfaces.CredentialsConstants;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status;

@Path("/authentication")
public class Authentication {

	private static final Logger log = LoggerFactory.getLogger(Authentication.class); 
	
	private void logRequest(final String method, final HttpServletRequest request) {
		log.info("REST " + method + ", from: " + request.getRemoteAddr());
	}
	
	@GET
	@Path("{protocol}")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response authentications(
			@Context HttpServletRequest request,
			@PathParam("protocol") String protocol) {
		try {
			logRequest("GET", request);
	
		    Adaptor adaptor;
		    try { adaptor = AdaptorRegistry.getAdaptorInstance(protocol); }
		    catch (NotSupportedProtocolException e) { return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build(); }
			
		    // [{displayName:"Access key-Secret key",keyName:UserPass,fields:[{keyName:UserID,displayName:"Access key"},{keyName:UserPass,displayName:"Secret key"}]}, ...]
		    AuthenticationTypeList adaptorAuthList = adaptor.getAuthenticationTypeList(protocol);
		    
		    JSONArray jsonArray = new JSONArray();
		    for (AuthenticationType auth: adaptorAuthList.getAuthenticationTypes()) {
		    	JSONObject authJSON = new JSONObject();
		    	authJSON.put(CredentialsConstants.TYPE, auth.getType()); // "type"
		    	authJSON.put("displayName", auth.getDisplayName());
		    	JSONArray fieldsJSON = new JSONArray();
		    	for (AuthenticationField field: auth.getFields()) {
		    		JSONObject fieldJSON = new JSONObject();
		    		fieldJSON.put("keyName", field.getKeyName());
		    		fieldJSON.put("displayName", field.getDisplayName());
		    		fieldJSON.put("defaultValue", field.getDefaultValue());
		    		fieldJSON.put("type", field.getType());
		    		fieldsJSON.put(fieldJSON);
		    	}
		    	authJSON.put("fields", fieldsJSON);
		    	jsonArray.put(authJSON);
		    }
		    
			return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonArray.toString()).build();
		
		} catch (Throwable e) { log.error(e.getMessage(), e); return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build(); }
	}
}