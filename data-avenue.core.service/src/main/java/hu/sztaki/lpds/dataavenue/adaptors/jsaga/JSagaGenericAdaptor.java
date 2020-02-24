package hu.sztaki.lpds.dataavenue.adaptors.jsaga;

import org.ogf.saga.context.Context;
import org.ogf.saga.error.AlreadyExistsException;
import org.ogf.saga.error.AuthenticationFailedException;
import org.ogf.saga.error.AuthorizationFailedException;
import org.ogf.saga.error.BadParameterException;
import org.ogf.saga.error.DoesNotExistException;
import org.ogf.saga.error.IncorrectStateException;
import org.ogf.saga.error.IncorrectURLException;
import org.ogf.saga.error.NoSuccessException;
import org.ogf.saga.error.NotImplementedException;
import org.ogf.saga.error.PermissionDeniedException;
import org.ogf.saga.error.TimeoutException;
import org.ogf.saga.file.File;
import org.ogf.saga.file.FileFactory;
import org.ogf.saga.file.FileInputStream;
import org.ogf.saga.file.FileOutputStream;
import org.ogf.saga.logicalfile.LogicalFile;
import org.ogf.saga.logicalfile.LogicalFileFactory;
import org.ogf.saga.monitoring.Metric;
import org.ogf.saga.namespace.Flags;
import org.ogf.saga.namespace.NSDirectory;
import org.ogf.saga.namespace.NSEntry;
import org.ogf.saga.namespace.NSFactory;
import org.ogf.saga.permissions.Permission;
import org.ogf.saga.task.Task;
import org.ogf.saga.task.TaskMode;
import org.ogf.saga.url.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ogf.saga.session.Session;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import hu.sztaki.lpds.dataavenue.core.interfaces.impl.DirEntryImpl;
import hu.sztaki.lpds.dataavenue.core.interfaces.impl.FileEntryImpl;
import hu.sztaki.lpds.dataavenue.core.interfaces.impl.SymLinkEntryImpl;
import hu.sztaki.lpds.dataavenue.core.interfaces.impl.URIImpl;
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
import hu.sztaki.lpds.dataavenue.interfaces.URIBase.URIType;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationNotSupportedException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationFieldImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationTypeImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationTypeListImpl;
import fr.in2p3.jsaga.adaptor.base.usage.Usage;
import fr.in2p3.jsaga.engine.config.ConfigurationException;
import fr.in2p3.jsaga.engine.descriptors.AdaptorDescriptors;
import fr.in2p3.jsaga.engine.descriptors.DataAdaptorDescriptor;
import fr.in2p3.jsaga.engine.descriptors.SecurityAdaptorDescriptor;
import fr.in2p3.jsaga.engine.schema.config.Protocol;
import fr.in2p3.jsaga.impl.file.copy.AbstractCopyTask;
import fr.in2p3.jsaga.impl.namespace.AbstractNSEntryImpl;
import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.*;
import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.*;

/**
 * JSagaGenericAdaptor
 */
@Deprecated
public class JSagaGenericAdaptor implements Adaptor {
	private static final Logger log = LoggerFactory.getLogger(JSagaGenericAdaptor.class);
	
	private String version = "1.1.1-SNAPSHOT";
	
	static final String JSAGA_SESSION_KEY = "jsaga_session";

	static final boolean AUTO_DETECT_GLOBUS_PROXY = true;
	
	// lfc, irods not yet supported
	// 
	// https://packages.microsoft.com/repos/vscode/
	private static final List<String> allowedProtocols = Arrays.asList(HTTP_PROTOCOL, HTTPS_PROTOCOL, SFTP_PROTOCOL, GSIFTP_PROTOCOL, SRM_PROTOCOL, IRODS_PROTOCOL, LFC_PROTOCOL);
	// MyProxy authentication is not yet supported (requires proxy to access MyProxy server) 
	private static final List<String> allowedAuthenticationTypes = Arrays.asList(NONE_AUTH, USERPASS_AUTH, SSH_AUTH, /*MYPROXY_AUTH,*/ VOMS_AUTH, /*VOMSMYPROXY_AUTH,*/ GLOBUS_AUTH, GLOBUSRFC3820_AUTH, GLOBUSLEGACY_AUTH);
	public static boolean CONVERT_CREDENTIAL_ATTRIBUTES_TO_TEMP_FILES = true; // false: for junit testing
	private List<String> supportedProtocols;
	private Map<String, List<String>> securityContexts;
	private SecurityContextHelper securityContextHelper; 
	private ConcurrentHashMap<String, Task<NSEntry, Void>> taskRegistry = new ConcurrentHashMap<String, Task<NSEntry, Void>>(); // jSAGA managed asynchronous tasks (map of: String localId -> Task)
	private ConcurrentHashMap<String, NSEntry> taskResourceRegistry = new ConcurrentHashMap<String, NSEntry>(); // resources hold by jSAGA managed asynchronous tasks - a single NSEntry (map of: String localId -> NSEntry) 
	
	static {
		// set system properties required by jSAGA
		try { 
			if (System.getProperty("saga.factory") == null) { // to avoid exception: "No SAGA factory name specified"
				System.setProperty("saga.factory", "fr.in2p3.jsaga.impl.SagaFactoryImpl");
				log.info("System property 'saga.factory' set to fr.in2p3.jsaga.impl.SagaFactoryImpl");
			}
		} catch (SecurityException x) {	log.error("Cannot set system property 'saga.factory'"); 	}

		try {
			String userHome = System.getProperty("user.home");
			if (userHome == null) { userHome = "/tmp"; System.setProperty("user.home", userHome); }

			java.io.File JSAGA_USER = new java.io.File(userHome, ".jsaga/");
			java.io.File JSAGA_VAR = new java.io.File(JSAGA_USER, "var/");
			boolean jsagaDirExists = true;
			try {
				if (!JSAGA_USER.exists() && !JSAGA_USER.mkdir()) jsagaDirExists = false;  
				if (!JSAGA_VAR.exists() && !JSAGA_VAR.mkdir()) jsagaDirExists = false;
			} catch (SecurityException e) {	jsagaDirExists = false;	}
			
			if (!jsagaDirExists) log.error("Cannot create ./jsaga directory in 'user.home': " + System.getProperty("user.home") + "");
			else log.info(".jsaga directory: " + System.getProperty("user.home") + "/.jsaga/");

		} catch (SecurityException x) {	log.error("Cannot get/set system property 'user.home'"); } // thrown at System.getProperty
	}
	
	public JSagaGenericAdaptor() {
		securityContextHelper = new SecurityContextHelper(getSupportedProtocols()); // after static initializer, which sets user.home if absent
	}
	
	@Override public String getName() { return "jSAGA Generic Adaptor"; }
	
	@Override public String getDescription() { return "Adaptor for accessing resources accessible via jSAGA API"; }
	
	@Override public String getVersion() { return version; }

	
	@Override public List<String> getSupportedProtocols() { // return jSAGA supported protocols
		if (this.supportedProtocols != null) return this.supportedProtocols; // lazy initialization
		
        AdaptorDescriptors adaptorsDescriptorTable;
		try { adaptorsDescriptorTable = AdaptorDescriptors.getInstance(); } 
		catch (ConfigurationException e) {
			log.error("Cannot read jSAGA protocols due to configuration exception: " + e.getMessage() + "", e);
			return Collections.<String>emptyList();
		}
        
		DataAdaptorDescriptor dataAdaptorsDescriptorTable = adaptorsDescriptorTable.getDataDesc();
        Protocol [] dataAdaptorsProtocols = dataAdaptorsDescriptorTable.getXML();
        Set<String> typeSet = new HashSet<String> ();
        for (Protocol dataAdaptorProtocol: dataAdaptorsProtocols) {
        	if (dataAdaptorProtocol.getType().contains("test")) continue; // skip test protocols
        	if (dataAdaptorProtocol.getType().startsWith("file")) continue; // skip file:// to avoid access to DataAvenue file system
        	typeSet.add(dataAdaptorProtocol.getType());
        }	
        
        List<String> typeList = new ArrayList<String>();
        
        // filter allowed protocols
        for (String protocol: typeSet) {
        	if (allowedProtocols.contains(protocol)) typeList.add(protocol);
        	else log.info("DataAvenue does not publish JSAGA implemented protocol: {}", protocol);
        }
        
        this.supportedProtocols = typeList;
        return this.supportedProtocols;
	}
	
	@Override public List<OperationsEnum> getSupportedOperationTypes(final String protocol) {
		List<OperationsEnum> supprotedOperations = new ArrayList<OperationsEnum>();
		if (HTTP_PROTOCOL.equals(protocol) || HTTPS_PROTOCOL.equals(protocol)) {
			supprotedOperations.add(LIST);
			supprotedOperations.add(INPUT_STREAM);
		} else {
			supprotedOperations.add(LIST);
			supprotedOperations.add(MKDIR);
			supprotedOperations.add(RMDIR);
			supprotedOperations.add(DELETE);
			supprotedOperations.add(RENAME); 
			supprotedOperations.add(INPUT_STREAM);
			supprotedOperations.add(OUTPUT_STREAM);
			// no permission change (yet)
		}
		return supprotedOperations;
	}
	
	private List<OperationsEnum> getSupportedOperationTypes(final String fromProtocol, final String toProtocol) {
		List<OperationsEnum> supprotedOperations = new ArrayList<OperationsEnum>();
		if (HTTP_PROTOCOL.equals(toProtocol) || HTTPS_PROTOCOL.equals(toProtocol)) {
		} else {
			supprotedOperations.add(COPY_FILE);
			supprotedOperations.add(MOVE_FILE);
			supprotedOperations.add(COPY_DIR);
			supprotedOperations.add(MOVE_DIR);
		}
		return supprotedOperations;
	}
	
	@Override public List<OperationsEnum> getSupportedOperationTypes(final URIBase fromURI, final URIBase toURI) {
		// no host specific operations
		return getSupportedOperationTypes(fromURI.getProtocol(), toURI.getProtocol());
	}
	
	@Override public List<String> getAuthenticationTypes(final String protocol) {
		if (protocol == null) throw new IllegalArgumentException("null");
		if (securityContexts == null) securityContexts = new Hashtable<String, List<String>>();
		if (securityContexts.get(protocol) != null) return securityContexts.get(protocol);
        
		AdaptorDescriptors adaptorsDescriptorTable;
		try { adaptorsDescriptorTable = AdaptorDescriptors.getInstance(); } 
		catch (ConfigurationException e) {
			log.error("Cannot read jSAGA security contexts due to configuration exception: " + e.getMessage() + "");
			List<String> result = Collections.<String>emptyList();
			securityContexts.put(protocol, result);
			return result;
		}
        
		DataAdaptorDescriptor dataAdaptorsDescriptorTable = adaptorsDescriptorTable.getDataDesc();
        Protocol [] dataAdaptorsProtocols = dataAdaptorsDescriptorTable.getXML();
        List<String> protocolContextList = new ArrayList<String>();
        for (Protocol dataAdaptorProtocol: dataAdaptorsProtocols) {
        	if (protocol.equals(dataAdaptorProtocol.getType())) {
        		String [] contexts = dataAdaptorProtocol.getSupportedContextType();
        		for (String contextName: contexts) {
        			// filter for allowed authentication types
        			if (allowedAuthenticationTypes.contains(contextName))
        				protocolContextList.add(contextName);
        		}
        	}
        }

        // disable UserPass from HTTP
//        if (HTTP_PROTOCOL.equals(protocol)) protocolContextList.remove(USERPASS_AUTH); // remove this line when HTTP userpass will work
        
        // sort more frequent auth types according to some sort of priority
        List<String> orderedProtocolContextList = new ArrayList<String>();
        if (protocolContextList.remove(NONE_AUTH)) orderedProtocolContextList.add(NONE_AUTH);
    	if (protocolContextList.remove(USERPASS_AUTH)) orderedProtocolContextList.add(USERPASS_AUTH);
//    	if (protocolContextList.remove(SSH_AUTH)) orderedProtocolContextList.add(SSH_AUTH);
    	if (protocolContextList.remove(GLOBUS_AUTH)) orderedProtocolContextList.add(GLOBUS_AUTH);
    	if (AUTO_DETECT_GLOBUS_PROXY) { // NOTE: supress legacy and rfc820!
    		if (protocolContextList.remove(GLOBUSLEGACY_AUTH));
    		if (protocolContextList.remove(GLOBUSRFC3820_AUTH));
    	} else {
    		if (protocolContextList.remove(GLOBUSLEGACY_AUTH)) orderedProtocolContextList.add(GLOBUSLEGACY_AUTH);
    		if (protocolContextList.remove(GLOBUSRFC3820_AUTH)) orderedProtocolContextList.add(GLOBUSRFC3820_AUTH);
    	}
    	if (protocolContextList.remove(MYPROXY_AUTH)) orderedProtocolContextList.add(MYPROXY_AUTH);
    	if (protocolContextList.remove(VOMS_AUTH)) orderedProtocolContextList.add(VOMS_AUTH);
//    	if (protocolContextList.remove(VOMSMYPROXY_AUTH)) orderedProtocolContextList.add(VOMSMYPROXY_AUTH);
    	orderedProtocolContextList.addAll(protocolContextList); // add the rest

    	// override protocols in the case of iRODS
    	if (protocol.equals(IRODS_PROTOCOL)) {
    		orderedProtocolContextList.clear();
    		orderedProtocolContextList.add(IRODS_USERPASS_AUTH);
    	}
    	
        securityContexts.put(protocol, orderedProtocolContextList);
		return orderedProtocolContextList;
	}
	
	@Override public AuthenticationTypeList getAuthenticationTypeList(String protocol) {
		
		if (protocol == null) throw new IllegalArgumentException("null");

        AuthenticationTypeList orderedProtocolContextList = new AuthenticationTypeListImpl();
        
		AdaptorDescriptors adaptorsDescriptorTable;
		try { adaptorsDescriptorTable = AdaptorDescriptors.getInstance(); } 
		catch (ConfigurationException e) {
			log.error("Cannot read jSAGA security contexts due to configuration exception: " + e.getMessage() + "");
			return orderedProtocolContextList;
		}
        
		DataAdaptorDescriptor dataAdaptorsDescriptorTable = adaptorsDescriptorTable.getDataDesc();
        Protocol [] dataAdaptorsProtocols = dataAdaptorsDescriptorTable.getXML();
        List<String> protocolContextList = new ArrayList<String>();
        for (Protocol dataAdaptorProtocol: dataAdaptorsProtocols) {
        	if (protocol.equals(dataAdaptorProtocol.getType())) {
        		String [] contexts = dataAdaptorProtocol.getSupportedContextType();
        		for (String contextName: contexts) {
        			// filter for allowed authentication types
        			if (allowedAuthenticationTypes.contains(contextName))
        				protocolContextList.add(contextName);
        		}
        	}
        }

        if (protocolContextList.remove(NONE_AUTH)) {
    		AuthenticationType a = new AuthenticationTypeImpl();
    		a.setType(NONE_AUTH);
    		a.setDisplayName("No authentication");
    		orderedProtocolContextList.getAuthenticationTypes().add(a);
        }
    	if (protocolContextList.remove(USERPASS_AUTH)) {
    		
    		AuthenticationType a = new AuthenticationTypeImpl();
    		a.setType(USERPASS_AUTH);
    		a.setDisplayName("Password authentication");
    		
    		AuthenticationField f = new AuthenticationFieldImpl();
    		f.setKeyName("UserID");
    		f.setDisplayName("Username");
    		a.getFields().add(f);

    		f = new AuthenticationFieldImpl();
    		f.setKeyName("UserPass");
    		f.setDisplayName("Password");
    		f.setType(AuthenticationField.PASSWORD_TYPE);

    		a.getFields().add(f);
    		
    		orderedProtocolContextList.getAuthenticationTypes().add(a);
    	}
    	if (protocolContextList.remove(GLOBUS_AUTH)) {
    		AuthenticationType a = new AuthenticationTypeImpl();
    		a.setType(GLOBUS_AUTH);
    		a.setDisplayName("Globus authentication");
    		AuthenticationField f = new AuthenticationFieldImpl();
    		f.setKeyName(Context.USERPROXY);
    		f.setDisplayName("User proxy");
    		a.getFields().add(f);
    		orderedProtocolContextList.getAuthenticationTypes().add(a);
    	}
    	
    	if (AUTO_DETECT_GLOBUS_PROXY) { // NOTE: supress legacy and rfc820!
    		if (protocolContextList.remove(GLOBUSLEGACY_AUTH));
    		if (protocolContextList.remove(GLOBUSRFC3820_AUTH));
    	} else {
    		if (protocolContextList.remove(GLOBUSLEGACY_AUTH)) {
        		AuthenticationType a = new AuthenticationTypeImpl();
        		a.setType(GLOBUSLEGACY_AUTH);
        		a.setDisplayName("Globus authentication");
        		AuthenticationField f = new AuthenticationFieldImpl();
        		f.setKeyName(Context.USERPROXY);
        		f.setDisplayName("User proxy");
        		a.getFields().add(f);
        		orderedProtocolContextList.getAuthenticationTypes().add(a);

    		}
    		if (protocolContextList.remove(GLOBUSRFC3820_AUTH)) {
        		AuthenticationType a = new AuthenticationTypeImpl();
        		a.setType(GLOBUSRFC3820_AUTH);
        		a.setDisplayName("Globus authentication");
        		AuthenticationField f = new AuthenticationFieldImpl();
        		f.setKeyName(Context.USERPROXY);
        		f.setDisplayName("User proxy");
        		a.getFields().add(f);
        		orderedProtocolContextList.getAuthenticationTypes().add(a);
    		}
    	}
    	if (protocolContextList.remove(MYPROXY_AUTH)) {
    		AuthenticationType a = new AuthenticationTypeImpl();
    		a.setType(MYPROXY_AUTH);
    		a.setDisplayName("MyProxy authentication");
    		
    		AuthenticationField f = new AuthenticationFieldImpl();
    		f.setKeyName(Context.SERVER);
    		f.setDisplayName("MyProxy server");
    		a.getFields().add(f);

    		f = new AuthenticationFieldImpl();
    		f.setKeyName(Context.USERID);
    		f.setDisplayName("MyProxy username");
    		a.getFields().add(f);

    		f = new AuthenticationFieldImpl();
    		f.setKeyName(Context.USERPASS);
    		f.setDisplayName("MyProxy password");
    		f.setType(AuthenticationField.PASSWORD_TYPE);
    		a.getFields().add(f);
    		
    		f = new AuthenticationFieldImpl();
    		f.setKeyName(Context.USERPROXY );
    		f.setDisplayName("X509up proxy");
    		a.getFields().add(f);

    		f = new AuthenticationFieldImpl();
    		f.setKeyName(Context.LIFETIME );
    		f.setDisplayName("Lifetime");
    		a.getFields().add(f);
    		
    		orderedProtocolContextList.getAuthenticationTypes().add(a);
    	}
    	
    	if (protocolContextList.remove(VOMS_AUTH)) {
    		AuthenticationType a = new AuthenticationTypeImpl();
    		a.setType(VOMS_AUTH);
    		a.setDisplayName("VOMS authentication");
    		AuthenticationField f1 = new AuthenticationFieldImpl();
    		f1.setKeyName(Context.USERPROXY);
    		f1.setDisplayName("User proxy (VOMS extended)");
    		a.getFields().add(f1);
    		orderedProtocolContextList.getAuthenticationTypes().add(a);
    	}
    	
//    	orderedProtocolContextList.addAll(protocolContextList); // add the rest

    	// override protocols in the case of iRODS
    	if (protocol.equals(IRODS_PROTOCOL)) {
    		orderedProtocolContextList.getAuthenticationTypes().clear();
    		AuthenticationType a = new AuthenticationTypeImpl();
    		a.setType(IRODS_USERPASS_AUTH);
    		a.setDisplayName("iRODS authentication");
    		
    		AuthenticationField f = new AuthenticationFieldImpl();
    		f.setKeyName("UserID");
    		f.setDisplayName("Username");
    		a.getFields().add(f);

    		f = new AuthenticationFieldImpl();
    		f.setKeyName("UserPass");
    		f.setDisplayName("Password");
    		f.setType(AuthenticationField.PASSWORD_TYPE);
    		a.getFields().add(f);
    		
    		f = new AuthenticationFieldImpl();
    		f.setKeyName(IRODS_RESOURCE);
    		f.setDisplayName("iRODS resource");
    		a.getFields().add(f);

    		f = new AuthenticationFieldImpl();
    		f.setKeyName(IRODS_ZONE);
    		f.setDisplayName("iRODS zone");
    		a.getFields().add(f);

    		orderedProtocolContextList.getAuthenticationTypes().add(a);
    	}

    	return orderedProtocolContextList;
	}
	
	@Override public String getAuthenticationTypeUsage(final String protocol, final String type) {
		if (protocol == null || type == null) throw new IllegalArgumentException("null argument");
		
		// return descriptive messages
		if (NONE_AUTH.equals(type)) {
			return "(no credential data required)";
		} else if (IRODS_USERPASS_AUTH.equals(type)) {
			return "<b>" + Context.USERID + "</b> (username), <b>" + Context.USERPASS + "</b> (password), <b>" + IRODS_RESOURCE + "</b> (iRODS resource), <b>" + IRODS_ZONE + "</b> (iRODS zone)";
		} else if (SSH_AUTH.equals(type)) {
			return "<b>" + Context.USERID + "</b> (username), <b>UserPrivateKey</b> (ssh private key), <b>UserPass</b> (for private key)";
		} else if (USERPASS_AUTH.equals(type)) {
			return "<b>" + Context.USERID + "</b> (username), <b>" + Context.USERPASS + "</b> (password)";
		} else if (GLOBUS_AUTH.equals(type) || GLOBUSLEGACY_AUTH.equals(type) || GLOBUSRFC3820_AUTH.equals(type)) { //type.startsWith("Globus")) 
			return "<b>" + Context.USERPROXY + "</b> (x509up proxy of type Globus/GlobusLegacy/GlobusRFC820)";
//				   "<b>" + Context.USERKEY + "</b> (userkey.pem), <b>" + Context.USERPASS + "</b> (password for userkey.pem), <b>" + Context.USERCERT + "</b> (usercert.pem), <b>" + Context.LIFETIME + "</b> (proxy delegation lifetime, optional)";
		} else if (MYPROXY_AUTH.equals(type)) {
			return "<b>" + Context.SERVER + "</b> (myproxy server), <b>" + Context.USERID + "</b> (myproxy username), <b>" + Context.USERPASS + "</b> (myproxy password), <b>" + Context.USERPROXY + "</b> (x509up proxy file to access myproxy server), <b>" + Context.LIFETIME + "</b> (proxy delegation lifetime)"; 
//				   "<b>" + Context.SERVER + "</b> (myproxy server), <b>" + Context.USERID + "</b> (myproxy username), <b>" + Context.USERPASS + "</b> (myproxy password), <b>"+ Context.USERKEY + "</b> (userkey.pem), <b>" + Context.USERPASS + "</b> (password for userkey.pem), <b>" + Context.USERCERT + "</b> (usercert.pem), <b>" + Context.LIFETIME + "</b> (proxy delegation lifetime)";
		} else if (VOMS_AUTH.equals(type)) {
			return "<b>" + Context.USERPROXY + "</b> (VOMS exteded x509 proxy file)"; 
//				   "<b>InitialUserProxy</b> (x509up proxy file without VOMS extension), <b>" + Context.SERVER + "</b> (VOMS server), <b>" + Context.USERVO + "</b> (user VO), or<br/><br/>" +
//				   "<b>" + Context.SERVER + "</b> (VOMS server + DN), <b>" + Context.USERVO + "</b> (user VO), <b>" + Context.USERKEY + "</b> (userkey.pem), <b>" + Context.USERPASS + "</b> (password for userkey.pem), <b>" + Context.USERCERT + "</b> (usercert.pem), <b>" + Context.LIFETIME + "</b> (proxy delegation lifetime)";
		} else if (VOMSMYPROXY_AUTH.equals(type)) {
		   	   return "<b>" + Context.SERVER + "</b> (VOMS server), <b>" + Context.USERVO + "</b> (user VO), <b>" + "MyProxyServer" + "</b> (myproxy server), <b>"  + "MyProxyUserID" + "</b> (myproxy username), <b>" + "MyProxyPass" + "</b> (myproxy password), <b>" + Context.USERPROXY + "</b> (x509up proxy file to access myproxy server), <b>" + "DelegationLifeTime" + "</b> (proxy delegation lifetime)"; 
		}
		
        AdaptorDescriptors adaptorsDescriptorTable;
		try { adaptorsDescriptorTable = AdaptorDescriptors.getInstance(); } 
		catch (ConfigurationException e) {
			log.error("Cannot read jSAGA security contexts due to configuration exception: (" + e.getMessage() + ")");
			return "n/a";
		}
        SecurityAdaptorDescriptor securityAdaptorsDescriptorTable = adaptorsDescriptorTable.getSecurityDesc();
		List<String> contextList = getAuthenticationTypes(protocol); 
		for (String contextName : contextList) {
			if (type.equals(contextName)) {
				Usage usage = securityAdaptorsDescriptorTable.getUsage(contextName);
				return usage != null ? usage.toString() : "n/a";
			}
		}
		return "n/a";
	}
	
	// get jSaga session from DataAvenue session, create a new one if not exists 
	private Session getJsagaSession(final DataAvenueSession dataAvenueSession) throws CredentialException {
		Session jSagaSession = null;
		if (dataAvenueSession != null) {
			CloseableJSagaSession cs = (CloseableJSagaSession) dataAvenueSession.get(JSAGA_SESSION_KEY);
			if (cs != null)	jSagaSession = cs.get();
		}
		if (jSagaSession == null) {
			log.trace("Creating new jSAGA session...");
			jSagaSession = securityContextHelper.createNewJSagaSession();
			
			if (dataAvenueSession != null) {
				dataAvenueSession.put(JSAGA_SESSION_KEY, new CloseableJSagaSession(jSagaSession));
				log.trace("jSAGA session stored in DataAvenue session");
			}
		} else {
			log.trace("Using existing jSAGA session");
		}
		return jSagaSession;
	}

	private void checkContextType(final String type, final String protocol) throws CredentialException {
		if (protocol == null) return;
		if (type == null) {
			if (getAuthenticationTypes(protocol).contains(NONE_AUTH)) return; // hashmap with size 0 (having null type)
			log.debug("null context type for protocol {}", protocol);
			throw new CredentialException("null context type " + type + " for protocol " + protocol);
		}
		// GlobusLegacy and GlobusRFX820 is treated as Globus
		if ((GLOBUS_AUTH.equals(type) || GLOBUSRFC3820_AUTH.equals(type) || GLOBUSLEGACY_AUTH.equals(type))) {
			if (!getAuthenticationTypes(protocol).contains(GLOBUS_AUTH)) {
				log.debug("Context type {} passed as {} is invalid for protocol {}", GLOBUS_AUTH, type, protocol);
				throw new CredentialException("Invalid context type " + type + " for protocol " + protocol);
			}
		} else if (!getAuthenticationTypes(protocol).contains(type)) {
			log.debug("Context type {} is invalid for protocol {}", type, protocol);
			log.debug("Valid context types are: " + getAuthenticationTypes(protocol));
			throw new CredentialException("Context type " + type + " is invalid for protocol " + protocol);
		}
	}
	
	private Session getJsagaSession(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession) throws URIException, CredentialException {
		Session jSagaSession = getJsagaSession(dataAvenueSession);
		// not data 
		if (credentials != null) {
			checkContextType(credentials.getCredentialAttribute(CredentialsConstants.TYPE), uri.getProtocol());
			securityContextHelper.addContextToSession(uri, credentials, jSagaSession);
		}
		return jSagaSession;
	}

	private Session getJsagaSession(final URIBase fromUri, final Credentials fromCredentials, final URIBase toUri, final Credentials toCredentials, final DataAvenueSession dataAvenueSession) throws URIException, CredentialException {
		Session jSagaSession = getJsagaSession(fromUri, fromCredentials, dataAvenueSession);
		assert jSagaSession != null;
		if (toCredentials != null) {
			checkContextType(toCredentials.getCredentialAttribute(CredentialsConstants.TYPE), toUri.getProtocol());
			securityContextHelper.addContextToSession(toUri, toCredentials, jSagaSession);
		}
		return jSagaSession;
	}
	
	@Override public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession dataAvenueSession) throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(uri, credentials, dataAvenueSession);
		
		URIImpl result = null;
		if (uri.getType() == URIType.DIRECTORY) { // get directory attributes
			result = new DirEntryImpl(uri.getURI());
			if (!SecurityContextHelper.HTTP_PROTOCOL.equals(uri.getProtocol()) && !SecurityContextHelper.HTTPS_PROTOCOL.equals(uri.getProtocol())) {
				URL url = URIImpl.toJSagaURL(uri.getURI());
				NSDirectory dir = null;
				try { 
					dir = NSFactory.createNSDirectory(session, result.getJSagaUrl(), Flags.NONE.getValue());
					((DirEntryImpl)result).setLastModified(dir.getMTime());
					
					// ====================
					//NOTE: permissions are not always determinable, and sometimes give data other than relevant to current user, so ignored
					Boolean readPermisssion = null, writePermission = null, executePermission = null;
					try {  //Note: "*": other, null: owner
						readPermisssion = dir.permissionsCheck(null, Permission.READ.getValue()) ? Boolean.TRUE : Boolean.FALSE;
						((DirEntryImpl)result).setOwnerReadPermission(readPermisssion);
					} 
					catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
					catch (Exception e) { log.warn("Cannot query Permission.READ of file entry: " + result.getURI() + "!", e); }
					
					try { 
						writePermission = dir.permissionsCheck(null, Permission.WRITE.getValue()) ? Boolean.TRUE : Boolean.FALSE;
						((DirEntryImpl)result).setOwnerWritePermission(writePermission);
					}
					catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
					catch (Exception e) { log.warn("Cannot query Permission.WRITE of file entry: " + result.getURI() + "!", e); }

					try { 
						executePermission = dir.permissionsCheck(null, Permission.EXEC.getValue()) ? Boolean.TRUE : Boolean.FALSE;
						((DirEntryImpl)result).setOwnerExecutePermission(executePermission);
					}
					catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
					catch (Exception e) { log.warn("Cannot query Permission.EXEC of file entry: " + result.getURI() + "!", e); }
					
					try { 
						readPermisssion = dir.permissionsCheck("*", Permission.READ.getValue()) ? Boolean.TRUE : Boolean.FALSE;
						((DirEntryImpl)result).setOthersReadPermission(readPermisssion);
						((DirEntryImpl)result).setGroupReadPermission(readPermisssion); // NOTE: group permissions assigned as others 
					}
					catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
					catch (Exception e) { log.warn("Cannot query Permission.READ of file entry: " + result.getURI() + "!", e); }
					
					try { 
						writePermission = dir.permissionsCheck("*", Permission.WRITE.getValue()) ? Boolean.TRUE : Boolean.FALSE;
						((DirEntryImpl)result).setOthersWritePermission(writePermission);
						((DirEntryImpl)result).setGroupWritePermission(writePermission); // NOTE: group permissions assigned as others
					}
					catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
					catch (Exception e) { log.warn("Cannot query Permission.WRITE of file entry: " + result.getURI() + "!", e); }

					try { 
						executePermission = dir.permissionsCheck("*", Permission.EXEC.getValue()) ? Boolean.TRUE : Boolean.FALSE;
						((DirEntryImpl)result).setOthersExecutePermission(executePermission);
					} 
					catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
					catch (Exception e) { log.warn("Cannot query Permission.EXEC of file entry: " + result.getURI() + "!", e); }
					
					// =====================
				}  
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException, NoSuccessException
				catch (DoesNotExistException e) {
					throw new URIException("URI does not exists: " + uri.getURI());
				}
				catch (Exception e) {
						log.trace("Cannot get last modification date of directory: " + url + " (" + e.getMessage() + ", " + e.getCause() + ")");
				} finally {
					try { if (dir != null) dir.close(); } 
					catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions
				}
			}
		} else if (uri.getType() == URIType.FILE) { // get file or symlink attributes
			result = new FileEntryImpl(uri.getURI());
			
			File f = null;
			try { 
				f = FileFactory.createFile(session, ((FileEntryImpl)result).getJSagaUrl(), Flags.NONE.getValue());
				
				try { ((FileEntryImpl)result).setLastModified(f.getMTime()); }
				catch (NotImplementedException e) {} catch (NoSuccessException e) {}
				catch (Exception e) { log.trace("Cannot get last modification date of file: " + result.getURI() + " (" + e.getMessage() + ", " + e.getCause() + ")"); }
				
				try { ((FileEntryImpl)result).setSize(f.getSize()); }
				catch (NotImplementedException e) {} catch (NoSuccessException e) {}
				catch (Exception e) { log.trace("Cannot get size of file: " + result.getURI() + " (" + e.getMessage() + ", " + e.getCause() + ")"); }
				
				Boolean readPermisssion = null, writePermission = null, executePermission = null;
				
				//NOTE: permissions are not always determinable, and sometimes give data other than relevant to current user, so ignored
				try {  //Note: "*": other, null: owner
					readPermisssion = f.permissionsCheck(null, Permission.READ.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOwnerReadPermission(readPermisssion);
				} 
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.READ of file entry: " + result.getURI() + "!", e); }
				
				try { 
					writePermission = f.permissionsCheck(null, Permission.WRITE.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOwnerWritePermission(writePermission);
				}
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.WRITE of file entry: " + result.getURI() + "!", e); }

				try { 
					executePermission = f.permissionsCheck(null, Permission.EXEC.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOwnerExecutePermission(executePermission);
				}
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.EXEC of file entry: " + result.getURI() + "!", e); }
				
				try { 
					readPermisssion = f.permissionsCheck("*", Permission.READ.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOthersReadPermission(readPermisssion);
					((FileEntryImpl)result).setGroupReadPermission(readPermisssion); // NOTE: group permissions assigned as others 
				}
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.READ of file entry: " + result.getURI() + "!", e); }
				
				try { 
					writePermission = f.permissionsCheck("*", Permission.WRITE.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOthersWritePermission(writePermission);
					((FileEntryImpl)result).setGroupWritePermission(writePermission); // NOTE: group permissions assigned as others
				}
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.WRITE of file entry: " + result.getURI() + "!", e); }

				try { 
					executePermission = f.permissionsCheck("*", Permission.EXEC.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOthersExecutePermission(executePermission);
				} 
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.EXEC of file entry: " + result.getURI() + "!", e); }
				
			} catch (BadParameterException x) {
					// lfc:// not physical file exception thrown
					// LFC what details we can obtain?
			} catch (Exception e) {
				log.trace("Cannot get details of file: " + result.getURI() + " (" + e.getMessage() + ", " + e.getCause() + ")");
			} finally {
				try { if (f != null) f.close(); } 
				catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions
			}
			
		} else if (uri.getType() == URIType.SYMBOLIC_LINK) { // get file or symlink attributes
			result = new SymLinkEntryImpl(uri.getURI());
			// no details here
		}
		if (result == null) throw new OperationException("Cannot get attributes URI of unknown type: " + uri.getURI());
		return result;
	}
	
	@Override public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession dataAvenueSession, List <String> subentires) throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(uri, credentials, dataAvenueSession);

		List<DirEntryImpl> subdirs = new ArrayList<DirEntryImpl>(); 
		List<FileEntryImpl> files = new ArrayList<FileEntryImpl>();
		List<SymLinkEntryImpl> symlinks = new ArrayList<SymLinkEntryImpl>();

		URL url = URIImpl.toJSagaURL(uri.getURI());
		
		NSDirectory dir;
		String basePath;
		try { 
			dir = NSFactory.createNSDirectory(session, url);
			basePath = dir.getURL().toString();
			if (!basePath.endsWith("/")) basePath += "/"; 
		} 
		catch (DoesNotExistException e) { throw new OperationException("Source file or directory does not exist!", e); } 
		catch (AlreadyExistsException e) { throw new OperationException("Directory already exists!", e); } // it should not happen with default Flags.NONE 
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen here (URIImpl.toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { e.printStackTrace(); throw new CredentialException("Authentication failed!", e); } // should not happen (session)
		catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); } 
		catch (BadParameterException e) { e.printStackTrace(); throw new URIException("Malformed URI: " + uri, e); } // should not happen here (URIImpl.toJSagaURL)
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } // sometimes, there is problem with CA (Unkown CA(
		catch (NoSuccessException e) { throw new OperationException(e); } // e.g. bad password on http connection (?)
		catch (IncorrectStateException e) {	throw new OperationException(e); } // ?
			
		assert dir != null;
		try {
			for (URL dirEntry: dir.list()) { // String pattern = "*"; dir.list(pattern);
				if (subentires != null && subentires.size() > 0 && !subentires.contains(dirEntry.getPath())) continue; // filter subentries
				String entryURL = basePath + dirEntry.getPath();
				try {
					if (dir.isDir(dirEntry)) subdirs.add(new DirEntryImpl(entryURL));
					else if (dir.isEntry(dirEntry)) files.add(new FileEntryImpl(entryURL));
					else if (dir.isLink()) symlinks.add(new SymLinkEntryImpl(entryURL));
				} catch (Exception e) {	log.warn("Cannot get type of entry: " + entryURL + "(" + e.getMessage() + ")"); }
			}
		} 
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen here (URIImpl.toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); } // should not happen (session)
		catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new URIException("Malformed URI: " + uri, e); } // should not happen here (URIImpl.toJSagaURL)
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); } // ?
		catch (IncorrectStateException e) {	throw new OperationException(e); } // ?
		finally { 
			try { dir.close(); } catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions
		}

		// get last modification date for dirs
		// jSaga does not support getMTime for dir entries, skip it
		if (!SecurityContextHelper.HTTP_PROTOCOL.equals(uri.getProtocol()) && !SecurityContextHelper.HTTPS_PROTOCOL.equals(uri.getProtocol())) {
			for (DirEntryImpl dirEntry: subdirs) {
				NSDirectory subdir = null;
				try { 
					subdir = NSFactory.createNSDirectory(session, dirEntry.getJSagaUrl(), Flags.NONE.getValue());
					dirEntry.setLastModified(subdir.getMTime());
				}  
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException, NoSuccessException
				catch (Exception e) {
					log.trace("Cannot get last modification date of subdirectory: " + dirEntry + " (" + e.getMessage() + ", " + e.getCause() + ")");
				} finally {
					try { if (subdir != null) subdir.close(); } 
					catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions
				}
			}
		}
			
		// get size and last modification date for file
		for (FileEntryImpl fileEntry: files) {
			File f = null;
			FileEntryImpl result = fileEntry;
			try { 
				f = FileFactory.createFile(session, fileEntry.getJSagaUrl(), Flags.NONE.getValue());
				
				try { fileEntry.setLastModified(f.getMTime()); }
				catch (NotImplementedException e) {} catch (NoSuccessException e) {}
				catch (Exception e) { log.trace("Cannot get last modification date of file: " + fileEntry + " (" + e.getMessage() + ", " + e.getCause() + ")"); }
				
				try { fileEntry.setSize(f.getSize()); }
				catch (NotImplementedException e) {} catch (NoSuccessException e) {}
				catch (Exception e) { log.trace("Cannot get size of file: " + fileEntry + " (" + e.getMessage() + ", " + e.getCause() + ")"); }
				
				Boolean readPermisssion = null, writePermission = null, executePermission = null;
				
				//NOTE: permissions are not always determinable, and sometimes give data other than relevant to current user, so ignored
				try {  //Note: "*": other, null: owner
					readPermisssion = f.permissionsCheck(null, Permission.READ.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOwnerReadPermission(readPermisssion);
				} 
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.READ of file entry: " + result.getURI() + "!", e); }
				
				try { 
					writePermission = f.permissionsCheck(null, Permission.WRITE.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOwnerWritePermission(writePermission);
				}
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.WRITE of file entry: " + result.getURI() + "!", e); }
					try { 
					executePermission = f.permissionsCheck(null, Permission.EXEC.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOwnerExecutePermission(executePermission);
				}
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.EXEC of file entry: " + result.getURI() + "!", e); }
				
				try { 
					readPermisssion = f.permissionsCheck("*", Permission.READ.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOthersReadPermission(readPermisssion);
				}
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.READ of file entry: " + result.getURI() + "!", e); }
				
				try { 
					writePermission = f.permissionsCheck("*", Permission.WRITE.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOthersWritePermission(writePermission);
				}
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.WRITE of file entry: " + result.getURI() + "!", e); }
					try { 
					executePermission = f.permissionsCheck("*", Permission.EXEC.getValue()) ? Boolean.TRUE : Boolean.FALSE;
					((FileEntryImpl)result).setOthersExecutePermission(executePermission);
				} 
				catch (NotImplementedException e) {} catch (NoSuccessException e) {} // silently ignore NotImplementedException
				catch (Exception e) { log.warn("Cannot query Permission.EXEC of file entry: " + result.getURI() + "!", e); }
				
			} catch (BadParameterException x) {
					// lfc:// not physical file exception thrown
					break; // LFC what details we can obtain?
			} catch (Exception e) {
				log.trace("Cannot get details of file: " + fileEntry + " (" + e.getMessage() + ", " + e.getCause() + ")");
			} finally {
				try { if (f != null) f.close(); } 
				catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions
			}
		}
			
		// symlinks: no details provided
		List<URIBase> result = new ArrayList<URIBase>();
		result.addAll(subdirs);
		result.addAll(files);
		result.addAll(symlinks);
		return result;	
	}
	
	@Override public List<URIBase> list(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession) throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(uri, credentials, dataAvenueSession);
		List<URIBase> list = list(uri, session);
		return list;
	}

	private List<URIBase> list(final URIBase uri, final Session session) throws URIException, OperationException, CredentialException {
		
		List<DirEntryImpl> subdirs = new ArrayList<DirEntryImpl>(); 
		List<FileEntryImpl> files = new ArrayList<FileEntryImpl>();
		List<SymLinkEntryImpl> symlinks = new ArrayList<SymLinkEntryImpl>();

		URL url = URIImpl.toJSagaURL(uri.getURI());
		
		NSDirectory dir;
		String basePath;
		try { 
			dir = NSFactory.createNSDirectory(session, url);
			basePath = dir.getURL().toString();
			if (!basePath.endsWith("/")) basePath += "/"; 
		} 
		catch (DoesNotExistException e) { throw new OperationException("Source file or directory does not exist!", e); } 
		catch (AlreadyExistsException e) { throw new OperationException("Directory already exists!", e); } // it should not happen with default Flags.NONE 
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen here (URIImpl.toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { e.printStackTrace(); throw new CredentialException("Authentication failed!", e); } // should not happen (session)
		catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); } 
		catch (BadParameterException e) { e.printStackTrace(); throw new URIException("Malformed URI: " + uri, e); } // should not happen here (URIImpl.toJSagaURL)
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } // sometimes, there is problem with CA (Unkown CA(
		catch (NoSuccessException e) { throw new OperationException(e); } // e.g. bad password on http connection (?)
		catch (IncorrectStateException e) {	throw new OperationException(e); } // ?
			
		assert dir != null;
		try {
			for (URL dirEntry: dir.list()) { // String pattern = "*"; dir.list(pattern);
				String entryURL = basePath + dirEntry.getPath();
				try {
					if (dir.isDir(dirEntry)) subdirs.add(new DirEntryImpl(entryURL));
					else if (dir.isEntry(dirEntry)) files.add(new FileEntryImpl(entryURL));
					else if (dir.isLink()) symlinks.add(new SymLinkEntryImpl(entryURL));
				} catch (Exception e) {	log.warn("Cannot get type of entry: " + entryURL + "(" + e.getMessage() + ")"); }
			}
		} 
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen here (URIImpl.toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); } // should not happen (session)
		catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new URIException("Malformed URI: " + uri, e); } // should not happen here (URIImpl.toJSagaURL)
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); } // ?
		catch (IncorrectStateException e) {	throw new OperationException(e); } // ?
		finally { 
			try { dir.close(); } catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions
		}
		
		List<URIBase> result = new ArrayList<URIBase>();
		result.addAll(subdirs);
		result.addAll(files);
		result.addAll(symlinks);
		return result;
	}
	
	@Override public void mkdir(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession) throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(uri, credentials, dataAvenueSession);
		mkdir(uri, session);
	}
	
	private void mkdir(final URIBase uri, final Session session) throws URIException, OperationException, CredentialException {
		URL url = URIImpl.toJSagaURL(uri.getURI());
		NSDirectory subdir;
		try {
			// CREATE directory with CREATEPARENTS, indicate if exists (EXCL => AlreadyExistsException)
			subdir = NSFactory.createNSDirectory(session, url, Flags.CREATE.or(Flags.CREATEPARENTS.or(Flags.EXCL)));
		} 
		catch (AlreadyExistsException e) { throw new OperationException("Directory already exists: " + url, e); }
		catch (DoesNotExistException e) { throw new OperationException(e); }  // should not happen (Flags.CREATE)
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen (URIImpl.toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new OperationException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new OperationException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new OperationException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new URIException("Malformed URI: " + uri, e); } // should not happen (URIImpl.toJSagaURL)
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); }
		
		assert subdir != null;
		
		try { subdir.close(); } 
		catch (NoSuccessException e) {}	catch (NotImplementedException e) {} // silently ignore close exceptions

	}

	@Override public void rmdir(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession)  throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(uri, credentials, dataAvenueSession);
		rmdir(uri, session);
	}

	private void rmdir(final URIBase uri, final Session session)  throws URIException, OperationException, CredentialException {
		URL url = URIImpl.toJSagaURL(uri.getURI());

		// open the directory that must exist (default: Flags.NONE)
		NSDirectory dir;
		try { dir = NSFactory.createNSDirectory(session, url); }
		catch (AlreadyExistsException e) { throw new OperationException("Directory already exists!", e); } // should not happed (Flags.NONE)
		catch (DoesNotExistException e) { throw new OperationException("Directory does not exist: " + url, e); }
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen (URIImpl.toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new OperationException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new OperationException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new OperationException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new URIException("Malformed URI: " + uri, e); } // should not happen (URIImpl.toJSagaURL)
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); }
			
		assert dir != null;

		try {
			// remove directory recursively (subdirs and files contained)
			dir.remove(url, Flags.RECURSIVE.getValue());
		} 
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen 
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new OperationException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new OperationException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new OperationException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new URIException("Malformed URI: " + uri, e); } // should not happen (URIImpl.toJSagaURL)
		catch (DoesNotExistException e) { throw new OperationException(e); }  // should not happen
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); } // ?
		catch (IncorrectStateException e) {	throw new OperationException(e); } // ?
		finally {
			try { dir.close(); } catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions

		}
	}

	@Override public void delete(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession) throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(uri, credentials, dataAvenueSession);
		delete(uri, session);
	}
	
	private void delete(final URIBase uri, final Session session) throws URIException, OperationException, CredentialException {
		URL url = URIImpl.toJSagaURL(uri.getURI());
		NSEntry f;
		try {
			f = NSFactory.createNSEntry(session, url);
		}
		catch (DoesNotExistException e) { throw new OperationException("File to be deleted does not exist!", e); }
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); } 
		catch (AlreadyExistsException e) { throw new OperationException(e); } // should not happed (default Flags.NONE)
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen (URIImpl.toJSagaURL)
		catch (BadParameterException e) { throw new OperationException(e); } // should not happen (URIImpl.toJSagaURL, no parameters)
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); } // thrown in the case of sftp (unable to connect to server), no deletion done
		
		try { f.remove(); }// "Removes this entry and closes it." - actually, the program never terminates without explicit close (see finally) 
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new OperationException(e); } // should not happen (no parameters)
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) {} // sftp delete throws this exception, but deletes the file
		catch (IncorrectStateException e) {	throw new OperationException(e); } // ?
		finally {
			try { f.close(); } catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions
		}
	}

	@Override public void permissions(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession, final String permissionsString) throws URIException, OperationException, CredentialException {
		//Session session = getJsagaSession(uri, credentials, dataAvenueSession);
		throw new OperationException("Operation not implemented!"); // TODO implement
	}
	
	
	@Override public InputStream getInputStream(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession) throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(uri, credentials, dataAvenueSession); // dataAvenueSession is null on httpAlias
		// note: now Globus is converted to GlogusLegacy depending on the proxy type on isReadable...
		InputStream is = getInputStream(uri, session);
		return is;
	}
	
	private InputStream getInputStream(final URIBase uri, final Session session) throws URIException, OperationException, CredentialException {
		if (uri.getType() == URIBase.URIType.DIRECTORY) throw new OperationException("Cannot get input stream of a directory!");
		URL url = URIImpl.toJSagaURL(uri.getURI());

		if (SecurityContextHelper.LFC_PROTOCOL.equals(uri.getProtocol())) {
			// check file exists, check replicas...
			LogicalFile file;
			try { file = LogicalFileFactory.createLogicalFile(session, url, Flags.NONE.getValue()); } // get
			catch (DoesNotExistException e) { throw new URIException("File does not exist: " + uri, e); }
			catch (IncorrectURLException e) { throw new URIException("Malformed URI: " + url, e); } // should not happen (toJSagaURL)
			catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); }
			catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); }
			catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); }
			catch (NoSuccessException e) { throw new OperationException("Cannot open file: " + url, e); } // ?
			catch (NotImplementedException e) { throw new OperationException("Cannot open file: " + url, e); } // ?
			catch (BadParameterException e) { throw new OperationException("Internal server error: bad parameter!", e); } // should not happen	
			catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); }
			catch (AlreadyExistsException e) { throw new OperationException("File already exists!", e); } // should not happen (Flags.NONE)

			List <URL> replicas;
			try { replicas = file.listLocations(); } 
			catch (Exception e) { throw new OperationException("Can't get replicas for logical file: " + url, e); }
				
			if (replicas.size() == 0) throw new OperationException("No replica found for logical file: " + url);

			url = replicas.get(0); // get the first replica, use the same credentials as for LFC (!)
		}
		
		// NOTE: not yet supported: StreamFactory.createStream(URLFactory.createURL(fromUri.toString())).getInputStream(); => NotImplementedException
		try { 
			log.debug("Opening input stream of resource: " + url);
			FileInputStream fis = FileFactory.createFileInputStream(session, url);
			return fis; 
			//return new CloseableJSagaInputStream(fis); 
		}
		catch (AlreadyExistsException e) { throw new OperationException("File already exists!", e); } // should not happen
		catch (IncorrectURLException e) { throw new URIException(e); } // should not happen (toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new OperationException(e); } // should not happen 
		catch (DoesNotExistException e) { throw new URIException("File does not exist (deleted?)", e); } // deleted between alias creation and open stream 
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); } // SRM throws it if file locked or something  
	}

	private OutputStream getOutputStream(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession) throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(uri, credentials, dataAvenueSession);
		OutputStream os = getOutputStream(uri, session);
		return os;
	}
	
	@Override public OutputStream getOutputStream(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession, long contentLength) throws URIException, OperationException, CredentialException {
		return getOutputStream(uri, credentials, dataAvenueSession);
	}
	
	@Override public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session, InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException, OperationNotSupportedException {
		throw new OperationNotSupportedException();
	}
	
	private OutputStream getOutputStream(final URIBase uri, final Session session) throws URIException, OperationException, CredentialException {
		if (uri.getType() == URIBase.URIType.DIRECTORY) throw new OperationException("Cannot get output stream of a directory!");
		URL url = URIImpl.toJSagaURL(uri.getURI());
		
		if (SecurityContextHelper.LFC_PROTOCOL.equals(uri.getProtocol())) {
			// check file exists, check replicas...
			LogicalFile file;
			try { file = LogicalFileFactory.createLogicalFile(session, url, Flags.NONE.getValue()); } // get
			catch (DoesNotExistException e) { throw new URIException("File does not exist: " + uri, e); }
			catch (IncorrectURLException e) { throw new URIException("Malformed URI: " + url, e); } // should not happen (toJSagaURL)
			catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); }
			catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); }
			catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); }
			catch (NoSuccessException e) { throw new OperationException("Cannot open file: " + url, e); } // ?
			catch (NotImplementedException e) { throw new OperationException("Cannot open file: " + url, e); } // ?
			catch (BadParameterException e) { throw new OperationException("Internal server error: bad parameter!", e); } // should not happen	
			catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); }
			catch (AlreadyExistsException e) { throw new OperationException("File already exists!", e); } // should not happen (Flags.NONE)

			List <URL> replicas;
			try { replicas = file.listLocations(); } 
			catch (Exception e) { throw new OperationException("Can't get replicas for logical file: " + url, e); }
				
			if (replicas.size() == 0) throw new OperationException("No replica found for logical file: " + url);

			url = replicas.get(0); // get the first replica, use the same credentials as for LFC (!)
		}
		
		try { 
			FileOutputStream fos;
			log.debug("Opening output stream for resource: " + url);
			//try { 
			
				fos = FileFactory.createFileOutputStream(session, url, false); // append false: creates the file or overwrites the previous one if exists 
			//} 
			//catch(AlreadyExistsException e) {  
				//fos = FileFactory.createFileOutputStream(session, url, true); // failover: it occurs in SRM, try to write in append mode => not supported by adaptor 
			//}
				
			return fos;	 
			//return new ClosableJSagaOutputStream(fos);   
		}
		catch (AlreadyExistsException e) { throw new OperationException("File already exists: " + url, e);	} // should not happen (except srm)
		catch (IncorrectURLException e) { throw new URIException(e); } // should not happen (toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new OperationException(e); } // should not happen 
		catch (DoesNotExistException e) { throw new URIException("File deleted", e); }  // deleted between alias creation and open stream 
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); } // ?
	}

	@Override public long getFileSize(URIBase uri, Credentials credentials, DataAvenueSession dataAvenueSession) throws URIException, OperationException, CredentialException {

		Session session = getJsagaSession(uri, credentials, dataAvenueSession);

		URL url = URIImpl.toJSagaURL(uri.getURI());
		
		long size = getFileSize(url, session);
		
		if (dataAvenueSession == null) { session.close(); }
		
		return size;
	}

	private long getFileSize(URL url, Session session) throws URIException, OperationException, CredentialException {
		File f;
		try { f = FileFactory.createFile(session, url, Flags.NONE.getValue()); }
		catch (AlreadyExistsException e) { throw new OperationException("Directory already exists!", e); }
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen 
		catch (NotImplementedException e) { throw new OperationException("Operation not supported!", e); } 
		catch (AuthenticationFailedException e) { throw new OperationException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new OperationException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new OperationException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new URIException("Malformed URL: " + url, e); } 
		catch (DoesNotExistException e) { throw new OperationException(e); }  // should not happen
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); } // ?

		// try to get file size
		try { return f.getSize(); }
		catch (NotImplementedException e) { throw new OperationException("Operation not supported!", e); } 
		catch (AuthenticationFailedException e) { throw new OperationException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new OperationException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new OperationException("Permission denied!", e); } 
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException("This is not a file: " + url, e); } // it is not a file but a directory (or sym link?)
		catch (IncorrectStateException e) {	throw new OperationException(e); } // ?
		finally { try { f.close(); } catch (NotImplementedException e) {} catch (NoSuccessException e) {} }
	}
	
	@Override public boolean isReadable(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession) throws URIException, CredentialException, OperationException {
		Session session = getJsagaSession(uri, credentials, dataAvenueSession);
		Boolean result = isReadable(uri, session);
		if (dataAvenueSession == null) session.close(); 
		return result;
	}
	
	private boolean isReadable(final URIBase uri, final Session session) throws URIException, CredentialException, OperationException {

		URL url = URIImpl.toJSagaURL(uri.getURI());

		// in the case of lfn:// BadParameterException thrown on permissionsCheck
		if (SecurityContextHelper.LFC_PROTOCOL.equals(uri.getProtocol())) {
			// check file exists, check replicas...
			LogicalFile file;
			try { file = LogicalFileFactory.createLogicalFile(session, url, Flags.NONE.getValue()); } // get
			catch (DoesNotExistException e) { return false; /*throw new URIException("File does not exist: " + uri, e); */}
			catch (IncorrectURLException e) { throw new URIException("Malformed URI: " + url, e); } // should not happen (toJSagaURL)
			catch (PermissionDeniedException e) { return false; /* throw new CredentialException("Permission denied!", e); */ }
			catch (AuthorizationFailedException e) { return false; /* throw new CredentialException("Authorization failed!", e); */}
			catch (AuthenticationFailedException e) { return false; /* throw new CredentialException("Authentication failed!", e); */}
			catch (NoSuccessException e) { return false; /* throw new OperationException("Cannot open file: " + url, e); */} // ?
			catch (NotImplementedException e) { throw new OperationException("Cannot open file: " + url, e); } // ?
			catch (BadParameterException e) { throw new OperationException("Internal server error: bad parameter!", e); } // should not happen	
			catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); }
			catch (AlreadyExistsException e) { throw new OperationException("File already exists!", e); } // should not happen (Flags.NONE)

			List <URL> replicas;
			try { replicas = file.listLocations(); } 
			catch (Exception e) { throw new OperationException("Can't get replicas for logical file: " + url, e); }
			
			if (replicas.size() == 0) throw new OperationException("No replica found for logical file: " + url);
			
			// take the first replica and continue to check its readablility with the _same_ credentials (!) as LFC
			url = replicas.get(0);
			//return true; // it just tests existence of the logical file and that is has replica
		}
		
		File f = null;
		try { f = FileFactory.createFile(session, url, Flags.NONE.getValue()); } // without Flags.NONE: NoSuccess exception
		catch (DoesNotExistException e) { return false; /* throw new URIException("File does not exist: " + uri, e);*/ }
		catch (IncorrectURLException e) { throw new URIException("Malformed URI: " + url, e); } // should not happen (toJSagaURL)
		catch (PermissionDeniedException e) { return false; /*throw new CredentialException("Permission denied!", e);*/ }
		catch (AuthorizationFailedException e) { return false; /*throw new CredentialException("Authorization failed!", e);*/ }
		catch (AuthenticationFailedException e) { return false; /*throw new CredentialException("Authentication failed!", e);*/ }
		catch (NoSuccessException e) {
			// sftp exception: Unable to connect to server caused by: verify: false
			// assume to be readable (we dont know...)
			return true;
			//throw new OperationException("Cannot open file: " + url, e); 
		} // ?
		catch (NotImplementedException e) { throw new OperationException("Cannot open file: " + url, e); } // ?
		catch (BadParameterException e) { throw new OperationException("Internal server error: bad parameter!", e); } // should not happen	
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); }
		catch (AlreadyExistsException e) { throw new OperationException("File already exists!", e); } // should not happen (Flags.NONE)
		
		assert f != null;
		
		boolean result;
		try { result = f.permissionsCheck(null, Permission.READ.getValue()); }
		catch (PermissionDeniedException e) { return false; /*throw new CredentialException("Permission denied!", e);*/ }
		catch (AuthorizationFailedException e) { return false; /*throw new CredentialException("Authorization failed!", e);*/ }
		catch (AuthenticationFailedException e) { return false; /*throw new CredentialException("Authentication failed!", e);*/ }
		catch (NoSuccessException e) { throw new OperationException(e); } // maybe readable but cannot be verified
		catch (NotImplementedException e) {
			// it happens in the case of http, which is readable, so don't thow exception
			log.warn("Getting file permissions is not impemented! Assumed to be readable... ({})", e.getMessage());
			result = true;
			//throw new OperationException("Getting file permissions is not impemented!", e); // maybe readable but cannot be verified
		}
		catch (BadParameterException e) { throw new OperationException("Internal server error: bad parameter!", e); } // lfn throws bad parameter exception	...
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); }
		finally {	
			try { f.close(); } catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions
		}
		
		if (result) log.debug("Resource " + url + " is readable");
		else log.debug("Resource " + url + " is NOT readable");
		
		return result;
	}
	
	@Override public boolean isWritable(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession) throws URIException, CredentialException, OperationException {
		// if dataAvenueSession is null, it indicates create and close a new jsaga session
		Session session = getJsagaSession(uri, credentials, dataAvenueSession);
		Boolean result = isWritable(uri, session);
		if (dataAvenueSession == null) session.close();
		return result;
	}

	private boolean isWritable(final URIBase uri, final Session session) throws URIException, CredentialException, OperationException {
		log.trace("Checking target resource's existence and write permission...");

		boolean result = false;
		
		URL url = URIImpl.toJSagaURL(uri.getURI());
		String protocol = uri.getProtocol(); 
		
		if (SecurityContextHelper.LFC_PROTOCOL.equals(uri.getProtocol())) { // check file exists, check replicas...
			LogicalFile file;
			try { file = LogicalFileFactory.createLogicalFile(session, url, Flags.NONE.getValue()); } // get
			catch (DoesNotExistException e) { throw new URIException("File does not exist: " + uri, e); }
			catch (IncorrectURLException e) { throw new URIException("Malformed URI: " + url, e); } // should not happen (toJSagaURL)
			catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); }
			catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); }
			catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); }
			catch (NoSuccessException e) { throw new OperationException("Cannot open file: " + url, e); } // ?
			catch (NotImplementedException e) { throw new OperationException("Cannot open file: " + url, e); } // ?
			catch (BadParameterException e) { throw new OperationException("Internal server error: bad parameter!", e); } // should not happen	
			catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); }
			catch (AlreadyExistsException e) { throw new OperationException("File already exists!", e); } // should not happen (Flags.NONE)

			List <URL> replicas;
			try { replicas = file.listLocations(); } 
			catch (Exception e) { throw new OperationException("Can't get replicas for logical file: " + url, e); }
				
			if (replicas.size() == 0) throw new OperationException("No replica found for logical file: " + url);
				
			// take the first replica and continue to check its writablility with the _same_ credentials (!) as LFC
			url = replicas.get(0);
			protocol = url.getScheme();
		}

		File f = null;

		// NOTE: in the case of SRM, if the file already exists => not writable, if does not exist => maybe...  
		if (SecurityContextHelper.SRM_PROTOCOL.equals(protocol)) { 
			try { f = FileFactory.createFile(session, url, Flags.NONE.getValue()); } // Note: without Flags.NONE => NoSuccess exception
			catch (DoesNotExistException e) { result = true; } 
			catch (Exception e) {} // any other exception will result in "not writable", result == false
			finally { 
				try { if (f != null) f.close(); } catch (Exception e) {} // silently ignore close exceptions
				f = null;
			}
		} else {
			
			// protocols other than SRM
			try { f = FileFactory.createFile(session, url, Flags.NONE.getValue()); } // note: without Flags.NONE => NoSuccess exception 
			catch (DoesNotExistException e) {} // this possibly happens, not an exception
			catch (IncorrectURLException e) { throw new URIException("Malformed URI: " + url, e); } // should not happen (toJSagaURL)
			catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); }
			catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); }
			catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); }
			catch (NotImplementedException e) { throw new OperationException("Cannot open file: " + url, e); } // ?
			catch (BadParameterException e) { throw new OperationException("Internal server error: bad parameter!", e); } // should not happen	
			catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); }
			catch (AlreadyExistsException e) { throw new OperationException("File already exists!", e); } // should not happen (Flags.NONE)
			catch (NoSuccessException e) {
				// in the case of SFTP, it is thrown sometimes, workaround: just ignore it, let's see what happens afterwards (Unable to connect to server caused by: verify: false)
				f = null; 
				//throw new OperationException("Cannot open file: " + url, e);
			}  
	
			if (f == null) { // file does not exist
				log.trace("Creating file: " + url + "");
				try {
					f = FileFactory.createFile(session, url, Flags.CREATE.or(Flags.BINARY.getValue()));
				}
				catch (DoesNotExistException e) { throw new OperationException(e); } // should not happen due to CREATE
				catch (IncorrectURLException e) { throw new URIException("Malformed URI: " + url, e); }
				catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); }
				catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); }
				catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); }
				catch (NoSuccessException e) {
					// maybe readable but cannot be verified, may throw unable to connect to server, assume yes
					//throw new OperationException("Cannot create file: " + url , e); 
				} 
				catch (NotImplementedException e) { throw new OperationException("Cannot create file: " + url, e); } // maybe readable but cannot be verified
				catch (BadParameterException e) { throw new OperationException("Internal server error: bad parameter!", e); } // should not happen	
				catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); }
				catch (AlreadyExistsException e) { throw new OperationException("File already exists!", e); } // should not happen
				finally {
					try { if (f != null) f.close(); } catch (Exception e) {} // silently ignore close exceptions
				}
				
				// NOTE: reading write permissions on the newly created file causes operation exception
				//try { f.permissionsAllow(null, Permission.WRITE.getValue()); } // try to add write permission, silently ignore exceptions; necessary?
				//catch (PermissionDeniedException e) {} 
				//catch (AuthorizationFailedException e) {}
				//catch (AuthenticationFailedException e) {}
				//catch (NoSuccessException e) {} 
				//catch (NotImplementedException e) {}
				//catch (BadParameterException e) {}	
				//catch (TimeoutException e) {}
				
				result = true;
				
			} else { // the file exists, f != null
				
				try { 
					result = f.permissionsCheck(null, Permission.WRITE.getValue());
					log.trace("Write permission: " + result);
				}
				catch (PermissionDeniedException e) { throw new CredentialException("Permission denied!", e); }
				catch (AuthorizationFailedException e) { throw new CredentialException("Authorization failed!", e); }
				catch (AuthenticationFailedException e) { throw new CredentialException("Authentication failed!", e); }
				catch (NoSuccessException e) { throw new OperationException(e); } // maybe writable but cannot be verified
				catch (NotImplementedException e) { throw new OperationException("Write file permission check not impemented!", e); } // maybe writable but cannot be verified
				catch (BadParameterException e) { throw new OperationException("Internal server error: bad parameter!", e); } // should not happen	
				catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); }
				finally {	
					try { f.close(); } catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions
				}
			}
		} // end if protocols other than SRM
		
		if (result) log.debug("Resource " + url + " is writable");
		else log.debug("Resource " + url + " is NOT writable"); // exists but not writable
		
		return result;
	}

	@Override public void rename(final URIBase uri, final String newFileOrDirName, final Credentials credentials, final DataAvenueSession dataAvenueSession) throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(uri, credentials, dataAvenueSession);
		rename(uri, newFileOrDirName, session);
	}
	
	private void rename(final URIBase uri, final String newFileOrDirName, final Session session) throws URIException, OperationException, CredentialException {
		// OVERWRITE 1 RECURSIVE 2 CREATE 8 EXCL 16 CREATEPARENTS 64

		// test existence of the new file or dir with new name
		URL newFileOrDir = URIImpl.toJSagaURL(URIImpl.getContainerDirUri(uri) + newFileOrDirName); // uri.getFullPath() removes file name, query string, fragment, and adds '/' to the end
		try {
			int FLAGS_BYPASSEXIST = 4096;
			NSEntry entry = NSFactory.createNSEntry(session, newFileOrDir, FLAGS_BYPASSEXIST); // See: http://grid.in2p3.fr/software/jsaga-dev/jsaga-engine/xref/fr/in2p3/jsaga/command/NamespaceTest.html
			boolean exists = entry instanceof AbstractNSEntryImpl ? ((AbstractNSEntryImpl)entry).exists() : false;
			try { entry.close(); } catch (NoSuccessException e) {} catch (NotImplementedException e) {} // silently ignore close exceptions
			if (exists) throw new OperationException("Target URI (" + newFileOrDir + ") already exists!"); 
		} 		
		catch (DoesNotExistException e) { throw new URIException("Target URI " + newFileOrDir + " already exists!", e);  } // it should not happen <= FLAGS_BYPASSEXIST
		catch (AlreadyExistsException e) { throw new OperationException("Target file or directory already exists!", e); } // it should not happen <= FLAGS_BYPASSEXIST
		catch (BadParameterException e) { throw new URIException("Malformed URI: " + newFileOrDir, e); } // should not happen here (URIImpl.toJSagaURL)
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen here (URIImpl.toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } // it should not happen (originalFileOrDir succeeded)
		catch (AuthenticationFailedException e) { throw new OperationException("Authentication failed!", e); } // ?
		catch (AuthorizationFailedException e) { throw new OperationException("Authorization failed!", e); } // ?
		catch (PermissionDeniedException e) { throw new OperationException("Permission denied!", e); } // ?
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); } // it should not happen (originalFileOrDir succeeded)
		
		URL url = URIImpl.toJSagaURL(uri.getURI());
		NSEntry originalFileOrDir = null;
		try { originalFileOrDir = (uri.getType() == URIType.DIRECTORY) ? 
							NSFactory.createNSDirectory(session, url, Flags.NONE.getValue()): 
							NSFactory.createNSEntry(session, url, Flags.NONE.getValue()); 
		} 
		catch (DoesNotExistException e) { throw new OperationException("Source file or directory does not exist!", e); } 
		catch (AlreadyExistsException e) { throw new OperationException("Directory already exists!", e); } // it should not happen with Flags.NONE 
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen here (URIImpl.toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new OperationException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new OperationException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new OperationException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new URIException("Malformed URI: " + uri, e); } // should not happen here (URIImpl.toJSagaURL)
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); } // ?
		
		try { 
			if (uri.getType() == URIType.DIRECTORY) originalFileOrDir.move(newFileOrDir, Flags.RECURSIVE.getValue()); // Flags.EXCL not allowed, Flags.CREATEPARENTS not allowed
			else originalFileOrDir.move(newFileOrDir, Flags.NONE.getValue()); // Flags.EXCL not allowed
		} 
		catch (AlreadyExistsException e) { throw new OperationException("Target file or directory already exists!", e); } 
		catch (IncorrectURLException e) { throw new OperationException(e); } // should not happen here (URIImpl.toJSagaURL)
		catch (NotImplementedException e) { throw new OperationException("Operation not implemented!", e); } 
		catch (AuthenticationFailedException e) { throw new OperationException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { throw new OperationException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { throw new OperationException("Permission denied!", e); } 
		catch (BadParameterException e) { throw new OperationException("New name is invalid (or invalid flag used for move)!", e); } 
		catch (DoesNotExistException e) { throw new OperationException("Target file or directory does not exist!", e); }  // should not exist... (EXCL)
		catch (TimeoutException e) { throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { throw new OperationException(e); } // ?
		catch (IncorrectStateException e) {	throw new OperationException(e); } // ? 
		finally { try { originalFileOrDir.close(); } catch (NoSuccessException e) {} catch (NotImplementedException e) {} } // silently ignore close exceptions
	}

	private int determineCopyMoveFlags(final URIBase fromUri, final boolean isDir, final Session session, final boolean isCopy, final boolean overwrite) throws URIException, OperationException {
		// OVERWRITE 1 RECURSIVE 2 CREATE 8 EXCL 16 CREATEPARENTS 64

		if (!isDir) { // copy/move file
			return overwrite ? 
					Flags.OVERWRITE.getValue() : 
					Flags.NONE.getValue(); 
		} else { // copy/move dir
			return overwrite ?
					Flags.OVERWRITE.or(Flags.RECURSIVE.getValue()) : // Flags not allowed for this method: 1
					Flags.RECURSIVE.getValue(); // Flags not allowed for this method: 64
		}
	}
	
	// Start an asynchronous copy task
	@Override public String copy(final URIBase fromUri, final Credentials fromCredentials, final URIBase toUri, final Credentials toCredentials, final boolean overwrite, final TransferMonitor monitor)
			throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(fromUri, fromCredentials, toUri, toCredentials, null); // create a new jsaga session, don't use DataAvenue session
		boolean isFromUriDir = fromUri.getType() == URIType.DIRECTORY; //isDir(fromUri, session);
		boolean isToUriDir = toUri.getType() == URIType.DIRECTORY;// isDir(toUri, session);
		int flags = determineCopyMoveFlags(fromUri, isFromUriDir, session, true, overwrite);
		
		try {
			return copyOrMoveAsync(fromUri, toUri, session, isFromUriDir, isToUriDir, false, flags, overwrite, monitor);
		}
		catch (URIException e) {
//			log.debug("Closing JSAGA session..."); 
			if (session != null) { try { session.close(); } catch(Exception x) { log.warn("Cannot close JSAGA session!", x); } }
			throw e;
		}
		catch (OperationException e) {
//			log.debug("Closing JSAGA session..."); 
			if (session != null) { try { session.close(); } catch(Exception x) { log.warn("Cannot close JSAGA session!", x); } }
			throw e;
		}
		catch (CredentialException e) {
//			log.debug("Closing JSAGA session..."); 
			if (session != null) { try { session.close(); } catch(Exception x) { log.warn("Cannot close JSAGA session!", x); } }
			throw e;
		}
		catch (Exception e) {
//			log.debug("Closing JSAGA session..."); 
			if (session != null) { try { session.close(); } catch(Exception x) { log.warn("Cannot close JSAGA session!", x); } }
			throw new OperationException(e);
		}
	}
	
	// Start an asynchronous move task
	@Override public String move(final URIBase fromUri, final Credentials fromCredentials, final URIBase toUri, final Credentials toCredentials, final boolean overwrite, final TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		Session session = getJsagaSession(fromUri, fromCredentials, toUri, toCredentials, null);  // create a new jsaga session, don't use DataAvenue session
		boolean isFromUriDir = fromUri.getType() == URIType.DIRECTORY; //isDir(fromUri, session);
		boolean isToUriDir = toUri.getType() == URIType.DIRECTORY; //isDir(toUri, session);
		int flags = determineCopyMoveFlags(fromUri, isFromUriDir, session, false, overwrite);
		try {
			return copyOrMoveAsync(fromUri, toUri, session, isFromUriDir, isToUriDir, true, flags, overwrite, monitor);
		}
		catch (URIException e) {
			if (session != null) { try { session.close(); } catch(Exception x) { log.warn("Cannot close JSAGA session!", x); } }
			throw e;
		}
		catch (OperationException e) {
			if (session != null) { try { session.close(); } catch(Exception x) { log.warn("Cannot close JSAGA session!", x); } }
			throw e;
		}
		catch (CredentialException e) {
			if (session != null) { try { session.close(); } catch(Exception x) { log.warn("Cannot close JSAGA session!", x); } }
			throw e;
		}
		catch (Exception e) {
			if (session != null) { try { session.close(); } catch(Exception x) { log.warn("Cannot close JSAGA session!", x); } }
			throw new OperationException(e);
		}
	}
	
	// common task creation for copy/move
	private String copyOrMoveAsync(final URIBase fromUri, final URIBase toUri, final Session session, final boolean isFromDir, final boolean isToDir, final boolean isMove, final int flags, final boolean overwrite, final TransferMonitor monitor)
			throws URIException, OperationException, CredentialException {

		long connectionStart = System.currentTimeMillis(); // for measurements
		
		// in the case of lfn copy in a different way...
		if (SecurityContextHelper.LFC_PROTOCOL.equals(toUri.getProtocol())) 
			return copyOrMoveAsyncLFC(fromUri, toUri, session, isFromDir, isToDir, isMove, flags, overwrite, monitor);
		
		URL fromUrl = URIImpl.toJSagaURL(fromUri.getURI());
		
		// auto-complete toUri if dir->dir copy and toUri misses terminating slash
		String toUriString = toUri.getURI();
		URL toUrl = URIImpl.toJSagaURL(toUriString + ((isFromDir || isToDir) && !toUriString.endsWith("/") ? "/" : ""));
		
		// connection start
		NSEntry fromEntry;
		try {
			fromEntry = isFromDir ?
					NSFactory.createNSDirectory(session, fromUrl, Flags.NONE.getValue()) : // if it is a dir use create NS dir
					NSFactory.createNSEntry(session, fromUrl, Flags.NONE.getValue()); // else entry 
		} 
		catch (AlreadyExistsException e) { monitor.failed(e.getMessage()); throw new OperationException("Directory already exists!", e); }
		catch (IncorrectURLException e) {  monitor.failed(e.getMessage()); throw new OperationException(e); } // should not happen 
		catch (NotImplementedException e) { monitor.failed(e.getMessage()); throw new OperationException("Operation not supported!", e); } 
		catch (AuthenticationFailedException e) { monitor.failed(e.getMessage()); throw new OperationException("Authentication failed!", e); } 
		catch (AuthorizationFailedException e) { monitor.failed(e.getMessage()); throw new OperationException("Authorization failed!", e); } 
		catch (PermissionDeniedException e) { monitor.failed(e.getMessage()); throw new OperationException("Permission denied!", e); } 
		catch (BadParameterException e) { monitor.failed(e.getMessage()); throw new URIException("Malformed URI: " + fromUri, e); } // getFullPath truncates subdir name
		catch (DoesNotExistException e) { monitor.failed(e.getMessage()); throw new OperationException(e); }  // should not happen
		catch (TimeoutException e) { monitor.failed(e.getMessage()); throw new OperationException("Connection timeout!", e); } 
		catch (NoSuccessException e) { monitor.failed(e.getMessage()); throw new OperationException(e); } // ?

		Task<NSEntry, Void> task = null;
		try {
			task = 
				isMove ? 
						fromEntry.move(TaskMode.TASK, toUrl, flags):  // overwrite OVERWRITE causes exception (fixed by Lionel)
						fromEntry.copy(TaskMode.TASK, toUrl, flags);  // TaskMode.ASYNC: calls Task.run() implicitely
		} catch (NotImplementedException e) { 
			monitor.failed("Copy or move operation is not implemented by the adaptor");
			try { fromEntry.close(); } catch (Exception x) {}
			throw new OperationException("Copy or move operation is not implemented by the adaptor", e);	
		}
			
		String taskId = task.getId(); // get local id 
		log.info("New copy/move task with adaptor-managed id: " + taskId);
	
		// try to add task monitoring callback
		try {
			Metric metric = task.getMetric(AbstractCopyTask.FILE_COPY_PROGRESS); // "file.copy.progress"
			metric.addCallback(new CopyProgressMonitor(monitor));
			log.trace("FILE_COPY_PROGRESS callback registered");
		} 
		catch (NotImplementedException e) { log.warn("Cannot register progress monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (AuthenticationFailedException e) { log.warn("Cannot register progress monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (AuthorizationFailedException e) { log.warn("Cannot register progress monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (PermissionDeniedException e) { log.warn("Cannot register progress monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (DoesNotExistException e) { log.warn("Cannot register progress monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (TimeoutException e) { log.warn("Cannot register progress monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (NoSuccessException e) { log.warn("Cannot register progress monitor for copy/move task! ("  + e.getMessage() + ")");	}
		catch (IncorrectStateException e) {	log.warn("Cannot register progress monitor for copy/move task! ("  + e.getMessage() + ")"); } // task.run()
		
		// try to add task state monitoring callback
		try {
			Metric metric = task.getMetric(Task.TASK_STATE); // "task.state"
			metric.addCallback(new CopyStateMonitor(task, taskId, taskRegistry, taskResourceRegistry, monitor, session));
			log.trace("TASK_STATE callback registered");
		} 
		catch (NotImplementedException e) { log.warn("Cannot register state monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (AuthenticationFailedException e) { log.warn("Cannot register state monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (AuthorizationFailedException e) { log.warn("Cannot register state monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (PermissionDeniedException e) { log.warn("Cannot register state monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (DoesNotExistException e) { log.warn("Cannot register state monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (TimeoutException e) { log.warn("Cannot register state monitor for copy/move task! ("  + e.getMessage() + ")");	} 
		catch (NoSuccessException e) { log.warn("Cannot register state monitor for copy/move task! ("  + e.getMessage() + ")");	}
		catch (IncorrectStateException e) {	log.warn("Cannot register state monitor for copy/move task! ("  + e.getMessage() + ")"); } // task.run()
		// try to get source file size
        try {
        	if (!isFromDir) 
        		monitor.setTotalDataSize(getFileSize(fromUrl, session));
        } catch (Exception e) {
        	log.debug("Cannot get file size of source file to be copied...");
        } // silently ignore if source file size cannot be retrieved

		taskRegistry.put(taskId, task); // register task for later query (getStatus)
		taskResourceRegistry.put(taskId, fromEntry); // register resources to be released when task completed
		try { 
			task.run();
			log.debug("JSAGA task started (run)...");
			
	        long connectionTime = System.currentTimeMillis() - connectionStart;
	        log.info("### Connection time: " + ((float)connectionTime/1000) + " (" + connectionTime + "ms)");
		} // start task
		catch (NoSuccessException e) {  try { fromEntry.close(); } catch (Exception x) {} monitor.failed("Cannot run task!"); throw new OperationException("Cannot run task!", e); } // task.run()
		catch (TimeoutException e) {  try { fromEntry.close(); } catch (Exception x) {}	monitor.failed("Cannot run task!"); throw new OperationException("Cannot run task!", e); } // task.run()
		catch (IncorrectStateException e) {	 try { fromEntry.close(); } catch (Exception x) {}	monitor.failed("Cannot run task!"); throw new OperationException("Cannot run task!", e);	} // task.run()
		catch (NotImplementedException e) {  try { fromEntry.close(); } catch (Exception x) {}	monitor.failed("Cannot run task!"); throw new OperationException("Cannot run task!", e); }

		return taskId;
	}
	
	private String copyOrMoveAsyncLFC(final URIBase fromUri, final URIBase toUri, final Session session, final boolean isFromDir, final boolean isToDir, final boolean isMove, final int flags, final boolean overwrite, final TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		URL fromUrl = URIImpl.toJSagaURL(fromUri.getURI());
		
		String toUriString = toUri.toString();
		// auto-complete toUri if dir->dir copy and toUri misses terminating slash
		URL toUrl = URIImpl.toJSagaURL(toUriString + ((isFromDir || isToDir) && !toUriString.endsWith("/") ? "/" : ""));

		if (isMove) {
			monitor.failed("Move operation is not supported to LFC target");
			throw new OperationException("Move operation is not supported to LFC target");
		}
		if (isFromDir) {
			monitor.failed("Directory copy or move operation is not supported to LFC target");
			throw new OperationException("Directory copy or move operation is not supported to LFC target");
		}
		
		// copy to dir is not supported by jSaga for lfn, add filename to toUrl
		if (isToDir) {
			toUrl = URIImpl.toJSagaURL(toUrl + fromUri.getEntryName());
		}
		
		// create logical
		// check file exists, check replicas...
		LogicalFile file;
		try { 
			 
			// create the file if it does not exist (don't throw AlreadyExistsException)
			// NOTE copy + !overwrite creates the file if absent and ADDS a new replica (without deleting existing replicas)
			file = LogicalFileFactory.createLogicalFile(session, toUrl, Flags.CREATE.getValue()); // Flags.NONE.getValue()
			// if (overwrite)  file = LogicalFileFactory.createLogicalFile(session, toUrl, Flags.CREATE.getValue()); 
			//else file = LogicalFileFactory.createLogicalFile(session, toUrl, Flags.CREATE.or(Flags.EXCL)); 
		} // get
		catch (DoesNotExistException e) { monitor.failed("File does not exist: "); throw new URIException("File does not exist: " + toUrl, e); }
		catch (IncorrectURLException e) { monitor.failed("Malformed URI: " + toUrl); throw new URIException("Malformed URI: " + toUrl, e); } // should not happen (toJSagaURL)
		catch (PermissionDeniedException e) { monitor.failed("Permission denied!"); throw new CredentialException("Permission denied!", e); }
		catch (AuthorizationFailedException e) { monitor.failed("Authorization failed!");  throw new CredentialException("Authorization failed!", e); }
		catch (AuthenticationFailedException e) { monitor.failed("Authentication failed!");  throw new CredentialException("Authentication failed!", e); }
		catch (NoSuccessException e) { monitor.failed("Cannot open file: " + toUrl);  throw new OperationException("Cannot open file: " + toUrl, e); } // ?
		catch (NotImplementedException e) { monitor.failed("Cannot open file: "); throw new OperationException("Cannot open file: " + toUrl, e); } // ?
		catch (BadParameterException e) { monitor.failed("Internal server error: bad parameter!"); throw new OperationException("Internal server error: bad parameter!", e); } // should not happen	
		catch (TimeoutException e) { monitor.failed("Connection timeout!"); throw new OperationException("Connection timeout!", e); }
		catch (AlreadyExistsException e) { monitor.failed("File already exists!"); throw new OperationException("File already exists!", e); } // should not happen (no EXCL)

		if (overwrite) { // remove previous replicas
			List <URL> replicas;
			try { replicas = file.listLocations(); } 
			catch (Exception e) { 
				monitor.failed("Can't get replicas for logical file: " + toUrl);
				try { file.close(); } catch (Exception x) {} // silently ignore close exceptions
				throw new OperationException("Can't get replicas for logical file: " + toUrl, e); 
			}
			try {
				if (replicas.size() > 0) log.debug("Deleting replicas of logical file: " + toUrl);
				for (URL location: replicas) file.removeLocation(location);
			} catch (Exception e) { 
				monitor.failed("Cannot remove replicas of logical file: " + toUrl);
				try { file.close(); } catch (Exception x) {} // silently ignore close exceptions
				throw new OperationException("Cannot remove replicas of logical file: " + toUrl, e); 
			}
		}
		
		try { file.addLocation(fromUrl); }
		catch (AuthenticationFailedException e) { monitor.failed("Authentication failed!"); throw new CredentialException("Authentication failed for replica location: " + fromUrl + ", using LFC credentials", e); }
		catch (PermissionDeniedException e) { monitor.failed("Permission denied!"); throw new CredentialException("Permission denied for replica location: " + fromUrl + ", using LFC credentials", e); }
		catch (AuthorizationFailedException e) { monitor.failed("Authorization failed!"); throw new CredentialException("Authorization failed for replica location: " + fromUrl + ", using LFC credentials", e); }
		catch (Exception e) { throw new OperationException("Cannot register replica for logical file: " + toUrl, e); }
		finally { try { file.close(); } catch (Exception e) {} } // silently ignore close exceptions			
		
		// create a dummy progress
		monitor.done();
		return UUID.randomUUID().toString(); // not used
	}
	
	// Cancel adaptor managed task
	@Override public void cancel(String id) throws TaskIdException, OperationException {
		if (!taskRegistry.containsKey(id)) throw new TaskIdException("Invalid local process id: " + id);
		Task<NSEntry, Void> task = taskRegistry.remove(id); // remove the task from task registry (not possible to query its state later)
		try {
			task.cancel(5); // cancel the task within the specified timeout (seconds)
			// this sets status of the task CANCELED and CopyMoveStateMonitor will release resources stored in taskResourceRegistry
			// the code below is not needed:
			// NSEntry resource = taskResourceRegistry.remove(id); // release task resources
			//if (resource != null)  // silently ignore if the resource is already released (during getProcessStatus)
			//	try { resource.close();	} catch (NoSuccessException e) {} catch (NotImplementedException e) {} 
		}
		catch (TimeoutException e) { throw new OperationException("TimeoutException", e); } 
		catch (NoSuccessException e) { throw new OperationException("NoSuccessException", e); }
		catch (NotImplementedException e) { throw new OperationException("NotImplementedException", e);	} 
		catch (IncorrectStateException e) {	throw new OperationException("IncorrectStateException", e);	}
	}

	@Override public void shutDown() {
	}
}