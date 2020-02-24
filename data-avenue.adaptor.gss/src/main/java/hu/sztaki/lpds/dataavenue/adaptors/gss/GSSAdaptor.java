package hu.sztaki.lpds.dataavenue.adaptors.gss;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationField;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationType;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationTypeList;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.CredentialsConstants;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum;
import hu.sztaki.lpds.dataavenue.interfaces.TransferMonitor;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationNotSupportedException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationFieldImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationTypeImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationTypeListImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.*;

/*
 * GSS adaptor for Data Avenue
 * GSS REST API doc: https://github.com/CloudiFacturing/docs-and-training/blob/master/service_APIs/api_refissh.md
 * URI example: gss://...
 */
public class GSSAdaptor implements Adaptor {
	private static final Logger log = LoggerFactory.getLogger(GSSAdaptor.class);
	private static final String GSS_PRPOTOCOL = "gss"; // gss://
	private static final String GSS_CLIENTS = "gss_clients";
	private static final String FILES_REST_RESOURCE ="files";
	private static final DateFormat gssDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private String version = "1.0.0";
	private static String PROPERTIES_FILE_NAME = "META-INF/data-avenue-gss-adaptor.properties"; 
	
	static final String TOKEN_AUTH = "token";
	static final String USERPASS_AUTH = "userPass";
	// credential keys
	static final String SESSION_TOKEN ="token", AUTH_MANAGER_URL = "authUrl", CLUSTER = "cluster";
	
	public GSSAdaptor() {
		try {
			Properties prop = new Properties();
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME);
			if (in == null) { log.warn("Cannot find properties file: " + PROPERTIES_FILE_NAME); } 
			else {
				try {
					prop.load(in);
					try { in.close(); } catch (IOException e) {}
					// no properties to read yet
					if (prop.get("version") != null) version = (String) prop.get("version");
				} catch (Exception e) { log.warn("Cannot load properties from file: " + PROPERTIES_FILE_NAME); }
			}
		} catch (Throwable e) { log.warn("Cannot read properties file: " + PROPERTIES_FILE_NAME); } 
	}
   
	private GSSClient getClient(final URIBase uri, final Credentials credentials, final DataAvenueSession session) throws CredentialException {
		GSSClients clients = session != null ? (GSSClients) session.get(GSS_CLIENTS) : null;

		if (clients == null) {
			if (credentials == null) throw new CredentialException("No GSS credentials provided");
			clients = new GSSClients();
			clients.add(uri, new GSSClient(uri, credentials));
			if (session != null) {
				log.debug("Adding " + GSS_CLIENTS + " to session");
				session.put(GSS_CLIENTS, clients);
			}
		}
		
		GSSClient client = clients.get(uri);
		if (client == null) throw new CredentialException("Cannot create GSS client " + "(" + clients + " " + clients.get(uri) + ")"); 
		return client; 
	}

	// =============================================================================================================
	// HELPER FUNCTIONS
	// =============================================================================================================

	// convert DA URI to GSS REST URL, e.g. gss://.../home/ -> https://.../datacenter/files/home/
	private String getRestUrl(URIBase uri, String cluster) {
		return "https://" + // replace gss://
				uri.getHost() + 
				(uri.getPort() != null ? ":" + uri.getPort() : "/") +
				(cluster != null && !"".equals(cluster) ? cluster + "/" : "") + 
				FILES_REST_RESOURCE +
				uri.getPath(); 
	}

	private String getErrorDetails(Response response, String responseString) {
		return "(HTTP " + response.getStatus() + (responseString != null && !"".equals(responseString) ? ": " + responseString : "") + ")";
	}

	@SuppressWarnings("unused")
	private boolean dirExists(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory");
		GSSClient gssClient = getClient(uri, credentials, session);
		try {
			String restUrl = getRestUrl(uri, gssClient.cluster);
			WebTarget webTarget = gssClient.jerseyClient.target(restUrl);
			Invocation.Builder invocationBuilder = webTarget
					.request()
					.header("X-Auth-Token", gssClient.sessionToken);
			Response response = invocationBuilder.head();
			String responseString = response.readEntity(String.class);
			if (response.getStatus() != 204) {
				if (response.getStatus() == 200) throw new Exception("Resource is a file (HTTP " + response.getStatus() + "):" + responseString);
				else if (response.getStatus() == 404) return false;
				else throw new Exception("HTTP error " + response.getStatus() + ": " + response.readEntity(String.class) + ")"); 
			}
			return true;
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new OperationException(e); 
		}
		finally { if (session == null && gssClient != null) try { gssClient.shutdown(); } catch (Exception x) {} }		
	}
	
	private boolean fileExists(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file");
		GSSClient gssClient = getClient(uri, credentials, session);
		try {
			String restUrl = getRestUrl(uri, gssClient.cluster);
			log.debug("Calling REST HEAD " + restUrl);
			log.debug("Auth-token: " + (gssClient.sessionToken != null && gssClient.sessionToken.length() > 5 ? gssClient.sessionToken.substring(0, 5) : "null or ''"));
			
			WebTarget webTarget = gssClient.jerseyClient.target(restUrl);
			Invocation.Builder invocationBuilder = webTarget
					.request()
					.header("X-Auth-Token", gssClient.sessionToken);
			
			Response response = invocationBuilder.head();
			log.debug("HTTP status: " + response.getStatus());
			String responseString = response.readEntity(String.class);
			
			if (response.getStatus() != 200) {
				if (response.getStatus() == 204) throw new Exception("Resource is a directory (HTTP " + response.getStatus() + "):" + responseString);
				else if (response.getStatus() == 404) return false;
				else throw new Exception("HTTP error " + response.getStatus() + ": " + response.readEntity(String.class) + ")"); 
			}
			return true;
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new OperationException(e); 
		}
		finally { if (session == null && gssClient != null) try { gssClient.shutdown(); } catch (Exception x) {} }		
	}

	// =============================================================================================================
	// ADAPTOR IMPLEMENTATION
	// =============================================================================================================
	/* adaptor meta information */
	@Override public String getName() { return "GSS Adaptor"; }
	@Override public String getDescription() { return "GSS Adaptor allows of connecting to HPC via refissh API"; }
	@Override public String getVersion() { return version; }
	
	@Override  public List<String> getSupportedProtocols() {
		List<String> result = new Vector<String>();
		result.add(GSS_PRPOTOCOL);
		return result;
	}
	
	@Override public List<OperationsEnum> getSupportedOperationTypes(String protocol) {
		List<OperationsEnum> result = new Vector<OperationsEnum>();
		result.add(LIST); 
		result.add(MKDIR);
		result.add(RMDIR);
		result.add(DELETE); 
		result.add(INPUT_STREAM);  
		result.add(OUTPUT_STREAM);
		return result;
	}
	
	@Override public List<OperationsEnum> getSupportedOperationTypes(final URIBase fromURI, final URIBase toURI) {
		return Collections.<OperationsEnum>emptyList();
	}

	@Override public List<String> getAuthenticationTypes(String protocol) {
		List<String> result = new Vector<String>();
		result.add(TOKEN_AUTH);
		result.add(USERPASS_AUTH);
		return result;
	}	
	
	@Override public AuthenticationTypeList getAuthenticationTypeList(String protocol) {
		AuthenticationTypeList l = new AuthenticationTypeListImpl();
		AuthenticationType a;
		AuthenticationField f;

		a = new AuthenticationTypeImpl();
		a.setType(TOKEN_AUTH);
		a.setDisplayName("Session token");

		f = new AuthenticationFieldImpl();
		f.setKeyName(CLUSTER);
		f.setDisplayName("Data cluster");
		f.setDefaultValue("refissh");
		a.getFields().add(f);

		f = new AuthenticationFieldImpl();
		f.setKeyName(SESSION_TOKEN);
		f.setDisplayName("Session token");
		a.getFields().add(f);
		
		l.getAuthenticationTypes().add(a);

		a = new AuthenticationTypeImpl();
		a.setType(USERPASS_AUTH);
		a.setDisplayName("Authentication manager");

		f = new AuthenticationFieldImpl();
		f.setKeyName(AUTH_MANAGER_URL);
		f.setDisplayName("Authentication manager URL");
		f.setDefaultValue("https://.../authManager/AuthManager");
		a.getFields().add(f);

		f = new AuthenticationFieldImpl();
		f.setKeyName(CLUSTER);
		f.setDisplayName("Data cluster");
		f.setDefaultValue("refissh");
		a.getFields().add(f);
		
		f = new AuthenticationFieldImpl();
		f.setKeyName(CredentialsConstants.PROJECT);
		f.setDisplayName("Project");
		a.getFields().add(f);
		
		f = new AuthenticationFieldImpl();
		f.setKeyName(CredentialsConstants.USERID);
		f.setDisplayName("Username");
		a.getFields().add(f);

		f = new AuthenticationFieldImpl();
		f.setKeyName(CredentialsConstants.USERPASS);
		f.setDisplayName("Password");
		f.setType(AuthenticationField.PASSWORD_TYPE);
		a.getFields().add(f);

		l.getAuthenticationTypes().add(a);

		return l;
	}	
	
	@Override public String getAuthenticationTypeUsage(String protocol, String authenticationType) {
		// not supported anymore, use getAuthenticationTypeList
		return null;
	}
	
	@Override public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		// no attributes provided by GSS, so just return uri with default attributes
		return uri;
	}
	
	@Override public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession session, List <String> subentires) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory");
		
		GSSClient gssClient = getClient(uri, credentials, session);
		List<URIBase> uriList;
		try {
			String restUrl = getRestUrl(uri, gssClient.cluster) + "?view=full"; // get file size details
			log.debug("Calling REST GET " + restUrl);
			log.debug("Auth-token: " + (gssClient.sessionToken != null && gssClient.sessionToken.length() >5 ? gssClient.sessionToken.substring(0, 5) : "null or ''"));
			
			WebTarget webTarget = gssClient.jerseyClient.target(restUrl);
			Invocation.Builder invocationBuilder = webTarget
					.request()
					.accept(MediaType.APPLICATION_JSON)
					.header("X-Auth-Token", gssClient.sessionToken);
			
			Response response = invocationBuilder.get();
			log.debug("HTTP status: " + response.getStatus());
			String responseString = response.readEntity(String.class);
			
			if (response.getStatus() != 200) {
				if (response.getStatus() == 404) throw new Exception("Resource doesn't exist " + getErrorDetails(response, responseString));
				else if (response.getStatus() == 403) throw new Exception("Authentication failed " + getErrorDetails(response, responseString));
				else throw new Exception("Unexpected HTTP status code " + getErrorDetails(response, responseString)); 
			}
			
			JSONArray list;
			try { list = new JSONArray(new JSONTokener(responseString)); }
			catch (JSONException e) { throw new Exception("Invalid JSON response: json array is expected"); }
			
			uriList = new Vector<URIBase>();
			for (int i = 0; i < list.length(); i++) {
				JSONObject entry = list.optJSONObject(i);
				if (entry == null) throw new Exception("Invalid JSON object in response: " + list.getString(i));
				String visualName = entry.getString("visualName");
				String type = entry.getString("type"); // FOLDER, FILE, NOTEXIST
				boolean isDirectory = "FOLDER".equalsIgnoreCase(type);
				DefaultURIBaseImpl listEntry = new DefaultURIBaseImpl(uri.getURI() + visualName + (isDirectory ? "/" : ""));
				if (entry.has("size")) listEntry.setSize(entry.getLong("size"));
				if (entry.has("lastModified")) {
					String lastModified = entry.getString("lastModified"); // e.g., "2018-11-07 14:09:08"
					Date date =  gssDateFormat.parse(lastModified); 
					listEntry.setLastModified(date.getTime());
				}
				uriList.add(listEntry);
			}
		}
		catch (Exception e) { throw new OperationException(e); }
		finally { if (session == null && gssClient != null) try { gssClient.shutdown(); } catch (Exception x) {} }
		
		if (subentires == null || subentires.size() == 0) return uriList; // return unfiltered entries with default attibutes
		else { // filter relevant entries
			List<URIBase> filteredUriList = new Vector<URIBase>();
			for (URIBase uriElement: uriList) if (subentires.contains(uriElement.getEntryName())) filteredUriList.add(uriElement); 
			return filteredUriList;
		}
	}
	
	@Override public List<URIBase> list(final URIBase uri, Credentials credentials,	DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory");
		GSSClient gssClient = getClient(uri, credentials, session);
		try {
			String restUrl = getRestUrl(uri, gssClient.cluster);
			log.debug("Calling REST GET " + restUrl);
			log.debug("Auth-token: " + (gssClient.sessionToken != null && gssClient.sessionToken.length() > 5 ? gssClient.sessionToken.substring(0, 5) : "null or ''"));
			
			WebTarget webTarget = gssClient.jerseyClient.target(restUrl);
			Invocation.Builder invocationBuilder = webTarget
					.request()
					.accept(MediaType.APPLICATION_JSON)
					.header("X-Auth-Token", gssClient.sessionToken);
			
			Response response = invocationBuilder.get();
			log.debug("HTTP status: " + response.getStatus());
			String responseString = response.readEntity(String.class);
			
			if (response.getStatus() != 200) {
				if (response.getStatus() == 404) throw new Exception("Resource doesn't exist " + getErrorDetails(response, responseString));
				else if (response.getStatus() == 403) throw new Exception("Authentication failed " + getErrorDetails(response, responseString));
				else throw new Exception("Unexpected HTTP status code " + getErrorDetails(response, responseString)); 
			}
			
			JSONArray list;
			try { list = new JSONArray(new JSONTokener(responseString)); }
			catch (JSONException e) { throw new Exception("Invalid JSON response: json array is expected"); }
			
			List<URIBase> result = new Vector<URIBase>();
			for (int i = 0; i < list.length(); i++) {
				JSONObject entry = list.optJSONObject(i);
				if (entry == null) throw new Exception("Invalid JSON object in response: " + list.getString(i));
				String visualName = entry.getString("visualName");
				String type = entry.getString("type"); // FOLDER, FILE, NOTEXIST
				boolean isDirectory = "FOLDER".equalsIgnoreCase(type);
				result.add(new DefaultURIBaseImpl(uri.getURI() + visualName + (isDirectory ? "/" : "")));
			}
			return result;
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new OperationException(e); 
		}
		finally { if (session == null && gssClient != null) try { gssClient.shutdown(); } catch (Exception x) {} }		
	}
	
	@Override public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory");
		GSSClient gssClient = getClient(uri, credentials, session);
		try {
			String restUrl = getRestUrl(uri, gssClient.cluster);
			// in the case of dirs, cut trailing /
			if (restUrl.endsWith("/")) restUrl = restUrl.substring(0, restUrl.length() - 1); 
			log.debug("Calling REST POST " + restUrl);
			log.debug("Auth-token: " + (gssClient.sessionToken != null && gssClient.sessionToken.length() > 5 ? gssClient.sessionToken.substring(0, 5) : "null or ''"));
			
			WebTarget webTarget = gssClient.jerseyClient.target(restUrl);
			Invocation.Builder invocationBuilder = webTarget
					.request()
					.accept(MediaType.APPLICATION_JSON)
					.header("Content-type", "application/directory")
					.header("X-Auth-Token", gssClient.sessionToken);
			
			Response response = invocationBuilder.post(Entity.entity("", "application/directory"));
			log.debug("HTTP status: " + response.getStatus());
			String responseString = response.readEntity(String.class);
			log.debug("HTTP entity: " + responseString);
			
			if (response.getStatus() != 201) {
				if (response.getStatus() == 400) throw new Exception("No Content-Type header was given " + getErrorDetails(response, responseString));
				else if (response.getStatus() == 405) throw new Exception("Folder already exists, parent folder doesn't exist, or POST was attempted in root folder and root folder is configured immutable " + getErrorDetails(response, responseString));
				else if (response.getStatus() == 403) throw new Exception("Authentication failed " + getErrorDetails(response, responseString));
				else throw new Exception("Unexpected HTTP status code " + getErrorDetails(response, responseString)); 
			}
			
			log.info("Directory created: " + uri.getPath());
			return;
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new OperationException(e); 
		}
		finally { if (session == null && gssClient != null) try { gssClient.shutdown(); } catch (Exception x) {} }		
	}

	@Override public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory");
		GSSClient gssClient = getClient(uri, credentials, session);
		try {
			String restUrl = getRestUrl(uri, gssClient.cluster);
			// in the case of dirs, cut trailing /
			if (restUrl.endsWith("/")) restUrl = restUrl.substring(0, restUrl.length() - 1); 
			log.debug("Calling REST DELETE " + restUrl);
			log.debug("Auth-token: " + (gssClient.sessionToken != null && gssClient.sessionToken.length() >5 ? gssClient.sessionToken.substring(0, 5) : "null or ''"));
			
			WebTarget webTarget = gssClient.jerseyClient.target(restUrl);
			Invocation.Builder invocationBuilder = webTarget
					.request()
					.accept(MediaType.APPLICATION_JSON)
					.header("X-Auth-Token", gssClient.sessionToken)
					.header("Content-type", "application/directory");
			
			Response response = invocationBuilder.delete();
			log.debug("HTTP status: " + response.getStatus());
			String responseString = response.readEntity(String.class);
			
			if (response.getStatus() != 204) {
				if (response.getStatus() == 404) throw new Exception("Resource doesn't exist " + getErrorDetails(response, responseString));
				else if (response.getStatus() == 403) throw new Exception("Authentication failed " + getErrorDetails(response, responseString));
				else throw new Exception("Unexpected HTTP status code " + getErrorDetails(response, responseString)); 
			}
			
			log.info("Directory deleted: " + uri.getPath());
			return;
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new OperationException(e); 
		}
		finally { if (session == null && gssClient != null) try { gssClient.shutdown(); } catch (Exception x) {} }		
	}

	@Override public void delete(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file");
		GSSClient gssClient = getClient(uri, credentials, session);
		try {
			String restUrl = getRestUrl(uri, gssClient.cluster);

			log.debug("Calling REST DELETE " + restUrl);
			log.debug("Auth-token: " + (gssClient.sessionToken != null && gssClient.sessionToken.length() >5 ? gssClient.sessionToken.substring(0, 5) : "null or ''"));
			
			WebTarget webTarget = gssClient.jerseyClient.target(restUrl);
			Invocation.Builder invocationBuilder = webTarget
					.request()
					.accept(MediaType.APPLICATION_JSON)
					.header("X-Auth-Token", gssClient.sessionToken);
			
			Response response = invocationBuilder.delete();
			log.debug("HTTP status: " + response.getStatus());
			String responseString = response.readEntity(String.class);

			if (response.getStatus() != 204) {
				if (response.getStatus() == 404) throw new Exception("Resource doesn't exist " + getErrorDetails(response, responseString));
				else if (response.getStatus() == 403) throw new Exception("Authentication failed " + getErrorDetails(response, responseString));
				else throw new Exception("Unexpected HTTP status code " + getErrorDetails(response, responseString)); 
			}
			
			log.info("File deleted: " + uri.getPath());
			return;
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new OperationException(e); 
		}
		finally { if (session == null && gssClient != null) try { gssClient.shutdown(); } catch (Exception x) {} }		
	}

	@Override public void permissions(final URIBase uri, final Credentials credentials, final DataAvenueSession session, final String permissionsString) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported");
	}
	
	@Override public void rename(URIBase uri, String newName, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported");
	}
	
	@Override public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file");
		GSSClient gssClient = getClient(uri, credentials, session);
		try {
			String restUrl = getRestUrl(uri, gssClient.cluster);
			log.debug("Calling REST GET " + restUrl);
			log.debug("Auth-token: " + (gssClient.sessionToken != null && gssClient.sessionToken.length() >5 ? gssClient.sessionToken.substring(0, 5) : "null or ''"));
			
			WebTarget webTarget = gssClient.jerseyClient.target(restUrl);
			Invocation.Builder invocationBuilder = webTarget
					.request()
					.header("X-Auth-Token", gssClient.sessionToken);

			InputStream response = invocationBuilder.get(InputStream.class);
			return new InputStreamWrapper(response, session, gssClient);
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new OperationException(e); 
		}
		finally { if (session == null && gssClient != null) try { gssClient.shutdown(); } catch (Exception x) {} }		
	}

	// note: the method will overwrite remote file if exists
	@Override public OutputStream getOutputStream(URIBase uri, Credentials credentials,	DataAvenueSession session, long size) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported"); // cannot be implemented like this because of PUT/POST
	}

	// write remote file from input stream
	// NOTE: no progress indication is possible (bytestransferred/total)
	@Override public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session, InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException, OperationNotSupportedException {
		if (contentLength < 0) throw new OperationException("Content length is unspecified (" + contentLength + ")");
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file");

		GSSClient gssClient = getClient(uri, credentials, session);
		Client streamingClient = gssClient.getFixedLengthStreamingClient();
		
		try {
			String restUrl = getRestUrl(uri, gssClient.cluster);
			log.debug("Calling REST POST (new)/PUT (overwrite) " + restUrl);
			log.debug("Auth-token: " + (gssClient.sessionToken != null && gssClient.sessionToken.length() >5 ? gssClient.sessionToken.substring(0, 5) : "null or ''"));
			
			// check file exists
			WebTarget headWebTarget = gssClient.jerseyClient.target(restUrl);
			Invocation.Builder invocationBuilder = headWebTarget
					.request()
					.header("X-Auth-Token", gssClient.sessionToken);
			Response headResponse = invocationBuilder.head();
			boolean isNew;
			if (headResponse.getStatus() == 200) isNew = false; // resource is a file
			else if (headResponse.getStatus() == 204) throw new Exception("Resource is a folder " + getErrorDetails(headResponse, headResponse.readEntity(String.class)));
			else if (headResponse.getStatus() == 404) isNew = true; // resource does not exist
			else throw new Exception("Unexpected HTTP status code " + getErrorDetails(headResponse, headResponse.readEntity(String.class))); 
			
			// send POST or PUT
			log.debug("Resource new: " + isNew);
			log.debug("Content-Length: " + contentLength);
			WebTarget postWebTarget = streamingClient.target(restUrl);
			invocationBuilder = postWebTarget
					.request()
					.header("X-Auth-Token", gssClient.sessionToken)
					.header("Content-Type", MediaType.APPLICATION_OCTET_STREAM_TYPE)
					.header("Content-Length", contentLength)
					;
			
			Response response =  isNew ? 
				invocationBuilder.post(Entity.entity(inputStream, MediaType.APPLICATION_OCTET_STREAM_TYPE)) : 
				invocationBuilder.put(Entity.entity(inputStream, MediaType.APPLICATION_OCTET_STREAM_TYPE));
				
			String responseString = response.readEntity(String.class);
			try { response.close(); } catch (Exception x) {}
			
			if (response.getStatus() != 201) {
				if (response.getStatus() == 400) throw new Exception("No Content-Type header was given " + getErrorDetails(response, responseString));
				else if (response.getStatus() == 405) throw new Exception("File already exists, parent folder doesn't exist, or POST was attempted in root folder and root folder is configured immutable " + getErrorDetails(response, responseString));
				else if (response.getStatus() == 403) throw new Exception("Authentication failed " + getErrorDetails(response, responseString));
				else throw new Exception("Unexpected HTTP status code " + getErrorDetails(response, responseString)); 
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new OperationException(e); 
		}
		finally {
			try { if (streamingClient != null) streamingClient.close(); } catch (Exception x) {}
			if (session == null && gssClient != null) try { gssClient.shutdown(); } catch (Exception x) {} 
		}		
    }
	
	@Override public String copy(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported");
	}

	@Override public String move(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported");
	}

	@Override public void cancel(String id) throws TaskIdException, OperationException {
		throw new OperationException("Operation not supported");
	}

	@Override public long getFileSize(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file");

		long size = -1; // unknown
		// GET $URL/files/some/file/or/folder?view=resinf
		GSSClient gssClient = getClient(uri, credentials, session);
		try {
			String restUrl = getRestUrl(uri, gssClient.cluster) + "?view=resinfo";

			log.debug("Calling REST GET " + restUrl); 
			log.debug("Auth-token: " + (gssClient.sessionToken != null && gssClient.sessionToken.length() >5 ? gssClient.sessionToken.substring(0, 5) : "null or ''"));
			
			WebTarget webTarget = gssClient.jerseyClient.target(restUrl);
			Invocation.Builder invocationBuilder = webTarget
					.request()
					.accept(MediaType.APPLICATION_JSON)
					.header("X-Auth-Token", gssClient.sessionToken);
			
			Response response = invocationBuilder.get();
			log.debug("HTTP status: " + response.getStatus());
			String responseString = response.readEntity(String.class);
			
			if (response.getStatus() != 200) {
				if (response.getStatus() == 404) throw new Exception("Resource doesn't exist " + getErrorDetails(response, responseString));
				else throw new Exception("Unexpected HTTP status code " + getErrorDetails(response, responseString)); 
			}
			
			try { 
				JSONObject fileDetails = new JSONObject(new JSONTokener(responseString)); 
				if (fileDetails.has("size")) size = fileDetails.getLong("size");
			}
			catch (JSONException e) { log.warn("Invalid JSON response: json array is expected"); }
		} catch (Exception e) { log.warn("Exception during getting file size", e); }

		return size;
	}

	// test if object is readable
	@Override public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		// not possible to determine it, assume true if exists
		return fileExists(uri, credentials, session);
	}

	@Override public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		// not possible to determine it, assume true if exists
		return fileExists(uri, credentials, session);
	}
	
	@Override public void shutDown() {}
}
