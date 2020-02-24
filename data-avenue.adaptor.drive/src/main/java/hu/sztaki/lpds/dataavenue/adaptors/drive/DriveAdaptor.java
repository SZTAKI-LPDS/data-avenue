package hu.sztaki.lpds.dataavenue.adaptors.drive;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




//import com.google.api.client.auth.oauth2.Credential;
//import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
//import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
//import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
//import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
//import com.google.api.client.util.store.FileDataStoreFactory;
//import com.google.api.services.drive.Drive;
//import com.google.api.services.drive.Drive.Files;
//import com.google.api.services.drive.DriveScopes;
//import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
//import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
//import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
//import com.google.api.client.http.FileContent;
//import com.google.api.client.http.HttpRequestInitializer;
//import com.google.api.client.http.HttpTransport;
//import com.google.api.client.http.javanet.NetHttpTransport;
//import com.google.api.client.json.JsonFactory;
//import com.google.api.client.json.jackson2.JacksonFactory;
//import com.google.api.services.drive.Drive;
//import com.google.api.services.drive.DriveScopes;
//import com.google.api.services.drive.model.File;
//import com.google.api.services.drive.model.FileList;

import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationField;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationType;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationTypeList;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
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

import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.*;

public class DriveAdaptor implements Adaptor {
	
	private static final Logger log = LoggerFactory.getLogger(DriveAdaptor.class);
	
	private String adaptorVersion = "1.0.0"; // default adaptor version
	
	static final String PROTOCOL_PREFIX = "jclouds-";
	static final List<String> PROTOCOLS = new Vector<String>(); //  = { "aws-s3", "azureblob", "hpcloud-objectstorage", "ninefold-storage", "cloudfiles-uk", "cloudfiles-us" };
	static final List<String> APIS = new Vector<String>();
	static final List<String> PROVIDERS = new Vector<String>();
	
	public static final String JCLOUDS_SESSION = "jclouds";

	static final String NONE_AUTH = "None"; 
	static final String USERPASS_AUTH = "UserPass";

	public DriveAdaptor() {
		String PROPERTIES_FILE_NAME = "META-INF/data-avenue-adaptor.properties"; // try to read version number
		try {
			Properties prop = new Properties();
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME); // emits no exception but null returned
			if (in == null) log.warn("Cannot find properties file: " + PROPERTIES_FILE_NAME); 
			else {
				try {
					prop.load(in);
					try { in.close(); } catch (IOException e) {}
					if (prop.get("version") != null) adaptorVersion = (String) prop.get("version");
				} catch (Exception e) { log.warn("Cannot load properties from file: " + PROPERTIES_FILE_NAME); }
			}
		} catch (Throwable e) { log.warn("Cannot read properties file: " + PROPERTIES_FILE_NAME); }
	}
	
	/* adaptor meta information */
	@Override public String getName() { return "Drive Adaptor"; }
	@Override public String getDescription() { return "Drive Adaptor allows of connecting to Google drive storages"; }
	@Override public String getVersion() { return adaptorVersion; }
	
	@Override  public List<String> getSupportedProtocols() {
		return PROTOCOLS;
	}
	
	@Override public List<OperationsEnum> getSupportedOperationTypes(String protocol) {
		List<OperationsEnum> result = new Vector<OperationsEnum>();
		result.add(LIST); 
		result.add(MKDIR);
		result.add(RMDIR);
		result.add(DELETE);
		result.add(INPUT_STREAM);  
		return result;
	}
	
	@Override public List<OperationsEnum> getSupportedOperationTypes(final URIBase fromURI, final URIBase toURI) {
		return Collections.<OperationsEnum>emptyList();
	}
	
	@Override public List<String> getAuthenticationTypes(String protocol) {
		List<String> result = new Vector<String>();
		// for all protocols
		result.add(USERPASS_AUTH);
		return result;
	}
	
	@Override public String getAuthenticationTypeUsage(String protocol,
			String authenticationType) {
		if (protocol == null || authenticationType == null) throw new IllegalArgumentException("null argument");
		if (USERPASS_AUTH.equals(authenticationType)) return "<b>UserID</b> (access key), <b>UserPass</b> (secret key)";
		return null;
	}

	@Override
	public AuthenticationTypeList getAuthenticationTypeList(String arg0) {
		
		AuthenticationTypeList l = new AuthenticationTypeListImpl();

		AuthenticationType a = new AuthenticationTypeImpl();
		
		a = new AuthenticationTypeImpl();
		a.setType(USERPASS_AUTH);
		a.setDisplayName("Drive authentication");
		
		AuthenticationField f1 = new AuthenticationFieldImpl();
		f1.setKeyName("UserID"); // "UserID"
		f1.setDisplayName("Username");
		a.getFields().add(f1);
		
		AuthenticationField f2 = new AuthenticationFieldImpl();
		f2.setKeyName("UserPass"); // "UserPass"
		f2.setDisplayName("Password");
		f2.setType(AuthenticationField.PASSWORD_TYPE);
		a.getFields().add(f2);
		
		l.getAuthenticationTypes().add(a);
		
		return l;
	}

	
	@Override public List<URIBase> list(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");		
	}

	@Override public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}
	
	@Override public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession session, List <String> subentires) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}
	
	@Override public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (!uri.getPath().endsWith(DriveURI.PATH_SEPARATOR)) throw new OperationException("URI must end with /!");
		throw new OperationException("Operation not supported!");
	}

	@Override public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (!uri.getPath().endsWith(DriveURI.PATH_SEPARATOR)) throw new OperationException("URI must end with /!");
		throw new OperationException("Operation not supported!");
	}

	@Override public void delete(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public void permissions(final URIBase uri, final Credentials credentials, final DataAvenueSession session, final String permissionsString) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}
	
	@Override public void rename(URIBase uri, String newName, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public OutputStream getOutputStream(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession, long contentLength) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session, InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException, IllegalArgumentException, OperationNotSupportedException {
		throw new OperationException("Operation not supported!");
	}

	@Override public String copy(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public String move(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public void cancel(String id) throws TaskIdException, OperationException {
		throw new OperationException("Operation not supported!");
	}

	@Override public long getFileSize(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	public boolean exists(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");	
	}
	
	// test if object is readable
	@Override public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return exists(uri, credentials, session);
	}

	@Override public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return exists(uri, credentials, session);
	}

	@Override public void shutDown() {
		// no resources to free up
	}

/*
	  static GoogleAuthorizationCodeFlow getFlow(String CLIENTSECRETS_LOCATION) throws IOException {
		HttpTransport httpTransport = new NetHttpTransport();
		JacksonFactory jsonFactory = new JacksonFactory();
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(
				jsonFactory,
				DriveAdaptor.class.getResourceAsStream(CLIENTSECRETS_LOCATION));
		List<String> SCOPES = Arrays.asList(
			      "https://www.googleapis.com/auth/drive.file",
			      "https://www.googleapis.com/auth/userinfo.email",
			      "https://www.googleapis.com/auth/userinfo.profile");
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				httpTransport, jsonFactory, clientSecrets, SCOPES)
				.setAccessType("offline").setApprovalPrompt("force").build();

		return flow;
	}
	  
	static Credential exchangeCode(String authorizationCode, String REDIRECT_URI, String CLIENTSECRETS_LOCATION)
			throws CredentialException {
		try {
			GoogleAuthorizationCodeFlow flow = getFlow(CLIENTSECRETS_LOCATION);
			GoogleTokenResponse response = flow
					.newTokenRequest(authorizationCode)
					.setRedirectUri(REDIRECT_URI).execute();
			return flow.createAndStoreCredential(response, null);
		} catch (IOException e) {
			System.err.println("An error occurred: " + e);
			throw new CredentialException("Code exchange exception");
		}
	}
	*/
	@SuppressWarnings("unused")
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

}