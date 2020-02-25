package hu.sztaki.lpds.dataavenue.adaptors.cdmi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.cdmi.api.CDMIConstants;
import hu.sztaki.lpds.cdmi.api.CDMIContainerObject;
import hu.sztaki.lpds.cdmi.api.CDMIDataObject;
import hu.sztaki.lpds.cdmi.api.CDMIOperationException;
import hu.sztaki.lpds.cdmi.api.CDMIURIException;
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
import static hu.sztaki.lpds.cdmi.api.CDMIConstants.*;

@SuppressWarnings("deprecation")
public class CDMIAdaptor implements Adaptor {
	private static final Logger log = LoggerFactory.getLogger(CDMIAdaptor.class);
	private String adaptorVersion = "1.0.0"; // default adaptor version
	public static final String CDMI_PRPOTOCOL = "cdmi"; // cdmi://
	public static final String CDMIS_PRPOTOCOL = "cdmis"; // cdmis://

	static final String NONE_AUTH = "None";
	static final String USERPASS_AUTH = "UserPass"; 
	
	
	public CDMIAdaptor() {
		String PROPERTIES_FILE_NAME = "META-INF/data-avenue-adaptor.properties"; // try to read version number
		try {
			Properties prop = new Properties();
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME); // emits no exception but null returned
			if (in == null) { log.warn("Cannot find properties file: " + PROPERTIES_FILE_NAME); } 
			else {
				try {
					prop.load(in);
					try { in.close(); } catch (IOException e) {}
					if (prop.get("version") != null) adaptorVersion = (String) prop.get("version");
				} catch (Exception e) { log.warn("Cannot load properties from file: " + PROPERTIES_FILE_NAME); }
			}
		} catch (Throwable e) {log.warn("Cannot read properties file: " + PROPERTIES_FILE_NAME); } 
	}
	
	/* adaptor meta information */
	@Override public String getName() { return "CDMI Adaptor"; }
	@Override public String getDescription() { return "CDMI Adaptor allows of connecting to cloud storages via CDMI interface"; }
	@Override public String getVersion() { return adaptorVersion; }
	@Override  public List<String> getSupportedProtocols() {
		List<String> result = new Vector<String>();
		result.add(CDMI_PRPOTOCOL);
		result.add(CDMIS_PRPOTOCOL);
		return result;
	}
	@Override public List<OperationsEnum> getSupportedOperationTypes(String protocol) {
		List<OperationsEnum> result = new Vector<OperationsEnum>();
		result.add(LIST); 
		result.add(MKDIR);
		result.add(RMDIR);
		result.add(DELETE); 
		result.add(RENAME);
		result.add(PERMISSIONS);
		result.add(INPUT_STREAM);  
		result.add(OUTPUT_STREAM);
		return result;
	}
	private List<OperationsEnum> getSupportedOperationTypes(final String fromProtocol, final String toProtocol) {
		List<OperationsEnum> supprotedOperations = new ArrayList<OperationsEnum>();
		supprotedOperations.add(COPY_FILE);
		supprotedOperations.add(MOVE_FILE);
		// dir copy/move not supported
		return supprotedOperations;
	}
	@Override public List<OperationsEnum> getSupportedOperationTypes(final URIBase fromURI, final URIBase toURI) {
		// copy file is supported between different S3 hosts
		return getSupportedOperationTypes(fromURI.getProtocol(), toURI.getProtocol());
	}
	@Override public List<String> getAuthenticationTypes(String protocol) {
		List<String> result = new Vector<String>();
		if (CDMI_PRPOTOCOL.equals(protocol)) result.add("None");
		else result.add("UserPass");
		return result;
	}	
	@Override public String getAuthenticationTypeUsage(String protocol,
			String authenticationType) {
		if (protocol == null || authenticationType == null) throw new IllegalArgumentException("null argument");
		if ("UserPass".equals(authenticationType)) return "<b>UserID</b> (access key), <b>UserPass</b> (secret key)";
		return null;
	}
	
	@Override
	public AuthenticationTypeList getAuthenticationTypeList(String arg0) {
		
		AuthenticationTypeList l = new AuthenticationTypeListImpl();

		AuthenticationType a = new AuthenticationTypeImpl();
		a.setType(NONE_AUTH);
		a.setDisplayName("No authentication");

		l.getAuthenticationTypes().add(a);
		
		a = new AuthenticationTypeImpl();
		a.setType(USERPASS_AUTH);
		a.setDisplayName("CDMI authentication");
		
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
	
	// here comes the point ==========================================================================================
	
	@SuppressWarnings("unused")
	private void release(HttpClient client, HttpRequestBase request) {
		request.releaseConnection();
	}
	
	@Override public List<URIBase> list(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		try {
			// DA URI
			String cdmiUri = uri.getURI(); // cdmi:// 
			if (!cdmiUri.endsWith(CDMIConstants.PATH_SEPARATOR)) cdmiUri += CDMIConstants.PATH_SEPARATOR;
			// HTTP client URI
			String httpUri = (CDMI_PRPOTOCOL.equals(uri.getProtocol()) ? "http://" : "https://") + uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "") + uri.getPath();
			if (!httpUri.endsWith(CDMIConstants.PATH_SEPARATOR)) httpUri += CDMIConstants.PATH_SEPARATOR;
			
			List<URIBase> result = new Vector<URIBase>();
			for (String child: CDMIContainerObject.list(httpUri)) {
				if (child.endsWith(CDMIConstants.PATH_SEPARATOR)) { // directory
					result.add(new CDMIURIImpl(cdmiUri + child));
				} else { // data object
					CDMIURIImpl fileEntry = new CDMIURIImpl(cdmiUri + child);
					result.add(fileEntry);
					try {
						Map<String, Object> metadata = CDMIDataObject.getCDMIMetaData(httpUri + child);
						
						String sizeString = (String) metadata.get(CDMIConstants.CDMI_METADATA_SIZE);
						if (sizeString != null) {
							try { 
								long size = Long.parseLong(sizeString);
								fileEntry.setSize(size);
							} catch (NumberFormatException  e) { log.warn("NumberFormatException: " + sizeString);}
						}
						
						String creationDateString = (String) metadata.get(CDMIConstants.CDMI_METADATA_CTIME);
						if (creationDateString != null) {
							try { 
								Calendar cal = javax.xml.bind.DatatypeConverter.parseDateTime(creationDateString);
								fileEntry.setLastModified(cal.getTimeInMillis());
							} catch (IllegalArgumentException  e) { log.warn("Date format exception: " + creationDateString);}
						}
					} catch (Exception e) {
						log.warn("Couldn't retrieve metadata for object: " + fileEntry.getURI());
					}
				} // end data object
			} // end for
			return result;
		} 
		catch (CDMIURIException e ) { throw new URIException(e); }
		catch (CDMIOperationException e ) { throw new OperationException(e); }
	}

	@Override public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		String cdmiHost = (CDMI_PRPOTOCOL.equals(uri.getProtocol()) ? "http://" : "https://") + uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
		
		// create container
        @SuppressWarnings("resource")
		HttpClient httpclient = new DefaultHttpClient();
		HttpPut httpput = new HttpPut(cdmiHost + uri.getPath()); // "http://localhost:8082/cdmi-server/TestContainer"
        httpput.setHeader("Content-Type", "application/cdmi-container");
        httpput.setHeader("X-CDMI-Specification-Version", CDMI_SPECIFICATION_VERSION);
        try {
        httpput.setEntity(new StringEntity("{ \"metadata\" : { } }"));
		} catch (Exception e) { throw new OperationException(e); }
        
        HttpResponse response = null;
        try {
        	response = httpclient.execute(httpput);
        } catch (Exception e) { throw new OperationException(e); }
        
        Header[] hdr = response.getAllHeaders();
        System.out.println("Headers : " + hdr.length);
        for (int i = 0; i < hdr.length; i++) {
            System.out.println(hdr[i]);
        }
        System.out.println("---------");
        System.out.println(response.getProtocolVersion());
        System.out.println(response.getStatusLine().getStatusCode());
        System.out.println(response.getStatusLine().getReasonPhrase());
        System.out.println(response.getStatusLine().toString());
        System.out.println("---------");
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            long len = entity.getContentLength();
            if (len != -1 && len < 2048) {
            	try {
                System.out.println(EntityUtils.toString(entity));
            	} catch (Exception e) { throw new OperationException(e); } 
            }
        }
	}

	@Override public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
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

	@Override public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session, InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException, OperationNotSupportedException {
		throw new OperationNotSupportedException();
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
		return 0l;
	}

	// test if object is readable
	@Override public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return true;
	}

	@Override public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return true;
	}

	@Override public URIBase attributes(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}
	
	@Override public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession session, List <String> subentires) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public void shutDown() {
	}
}