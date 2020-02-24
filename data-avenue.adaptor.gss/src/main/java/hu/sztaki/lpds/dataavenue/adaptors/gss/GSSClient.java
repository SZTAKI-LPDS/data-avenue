package hu.sztaki.lpds.dataavenue.adaptors.gss;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.CredentialsConstants;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;

public class GSSClient {
	
	private static final Logger log = LoggerFactory.getLogger(GSSClient.class);

	String cluster;
	String sessionToken;
	Client jerseyClient = null;
 
	GSSClient(final URIBase uri, final Credentials credentials) throws CredentialException {
		String authType = credentials.getCredentialAttribute(CredentialsConstants.TYPE);
		if (GSSAdaptor.USERPASS_AUTH.equals(authType)) {
			// get session token with a WS call
			this.cluster = credentials.getCredentialAttribute(GSSAdaptor.CLUSTER);
			this.sessionToken = getSessionToken(
					credentials.getCredentialAttribute(GSSAdaptor.AUTH_MANAGER_URL),
					credentials.getCredentialAttribute(CredentialsConstants.PROJECT),
					credentials.getCredentialAttribute(CredentialsConstants.USERID),
					credentials.getCredentialAttribute(CredentialsConstants.USERPASS)
					);
		} else if (GSSAdaptor.TOKEN_AUTH.equals(authType)) {
			this.sessionToken = credentials.getCredentialAttribute(GSSAdaptor.SESSION_TOKEN);
			this.cluster = credentials.getCredentialAttribute(GSSAdaptor.CLUSTER);
		} else {
			throw new CredentialException("Invalid or missing authentication type");
		}
		if (sessionToken == null) throw new CredentialException("Cannot obtain session token");
		if (jerseyClient == null) jerseyClient = ClientBuilder.newClient(); 
	}

	void shutdown() {
		// release client resources
		if (jerseyClient != null) {
			try { jerseyClient.close(); } catch (Exception e) { log.warn(e.getMessage(), e); }
		}
	}
	
	private static final String tokenRequestXmlTemplate = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
			"<SOAP-ENV:Envelope xmlns:ns0=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ns1=\"http://authmanager.sintef.no/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"><SOAP-ENV:Header/><ns0:Body><ns1:getSessionToken><ns1:username>##USERNAME##</ns1:username><ns1:password>##PASSWORD##</ns1:password><ns1:project>##PROJECT##</ns1:project></ns1:getSessionToken></ns0:Body></SOAP-ENV:Envelope>";

	private String getSessionToken(String authManagerUrl, String project, String username, String password) throws CredentialException {
		if (authManagerUrl == null) throw new CredentialException("No authentication manager URL provided");
		if (username == null) throw new CredentialException("No username provided");
		if (password == null) throw new CredentialException("No password provided");
		if (project == null) throw new CredentialException("No project provided");
		jerseyClient = ClientBuilder.newClient();

		try {
			WebTarget webTarget = jerseyClient.target(authManagerUrl);
			Invocation.Builder invocationBuilder = webTarget
					.request()
					.accept(MediaType.APPLICATION_JSON);
			
			String xmlMessage = new String(tokenRequestXmlTemplate
					.replaceAll("##PROJECT##", project)
					.replaceAll("##USERNAME##", username)
					.replaceAll("##PASSWORD##", password).getBytes(), 
					"UTF-8" /*StandardCharsets.UTF_8*/);
			log.debug("Endpoint: " + authManagerUrl);
			Response response = invocationBuilder.post(Entity.entity(xmlMessage, "text/xml;charset=UTF-8"), Response.class);
			String responseString = response.readEntity(String.class);
			if (response.getStatus() != 200) throw new CredentialException("Failed to obtain session token: HTTP " + response.getStatus() + ", " + responseString);
			int from = responseString.indexOf("<return>"); 
			int to = responseString.indexOf("</return>");
			if (from == -1 || to == -1 || from > to) throw new CredentialException("Invalid XML response from authentication manager (no element <return> found)");
			from += "<return>".length();
			// token length 183
			return responseString.substring(from, to);
		} catch (Exception e) {
			throw new CredentialException(e);
		}
	}
	
	/*
	 * Use to upload large files with size known in advance.
	 * (Causes no Java heap exception.)
	 */
	Client getFixedLengthStreamingClient() {
		ClientConfig clientConfig = new ClientConfig();
		System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
		clientConfig.property(HttpUrlConnectorProvider.USE_FIXED_LENGTH_STREAMING, "true"); // "jersey.config.client.httpUrlConnector.useFixedLengthStreaming"
		Client client = ClientBuilder.newBuilder().withConfig(clientConfig).build();
		return client;
	}	
}