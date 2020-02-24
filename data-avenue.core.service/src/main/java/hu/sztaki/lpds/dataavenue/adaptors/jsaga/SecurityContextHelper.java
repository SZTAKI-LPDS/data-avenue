package hu.sztaki.lpds.dataavenue.adaptors.jsaga;

import fr.in2p3.jsaga.adaptor.security.VOMSContext;
import fr.in2p3.jsaga.impl.session.SessionImpl;
import hu.sztaki.lpds.dataavenue.core.Configuration;
import hu.sztaki.lpds.dataavenue.core.interfaces.impl.URIImpl;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.CredentialsConstants;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;

import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.util.ProxyCertificateUtil;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ogf.saga.context.Context;
import org.ogf.saga.context.ContextFactory;
import org.ogf.saga.error.AuthenticationFailedException;
import org.ogf.saga.error.AuthorizationFailedException;
import org.ogf.saga.error.BadParameterException;
import org.ogf.saga.error.DoesNotExistException;
import org.ogf.saga.error.IncorrectStateException;
import org.ogf.saga.error.NoSuccessException;
import org.ogf.saga.error.NotImplementedException;
import org.ogf.saga.error.PermissionDeniedException;
import org.ogf.saga.error.TimeoutException;
import org.ogf.saga.session.Session;
import org.ogf.saga.session.SessionFactory;
import org.ogf.saga.url.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

@SuppressWarnings("deprecation")
public class SecurityContextHelper {
	
	private static final Logger log = LoggerFactory.getLogger(SecurityContextHelper.class);
	
	// protocols
	public static final String HTTP_PROTOCOL = "http";
	public static final String HTTPS_PROTOCOL = "https";
	public static final String SFTP_PROTOCOL = "sftp"; // requires disabling host key checking
	public static final String GSIFTP_PROTOCOL = "gsiftp";
	public static final String SRM_PROTOCOL = "srm";
	public static final String LFC_PROTOCOL = "lfn";
	public static final String IRODS_PROTOCOL = "irods";
//	public static final String SRB_PROTOCOL = "srb"; // not supported at the moment
	
	// authentication types
	static final String NONE_AUTH = "None";
	static final String USERPASS_AUTH = "UserPass";
	static final String SSH_AUTH = "SSH";  
	static final String MYPROXY_AUTH = "MyProxy";
	static final String GLOBUS_AUTH = "Globus";
	static final String GLOBUSRFC3820_AUTH = "GlobusRFC820";
	static final String GLOBUSLEGACY_AUTH = "GlobusLegacy";
	static final String VOMS_AUTH = "VOMS";
	static final String VOMSMYPROXY_AUTH = "VOMSMyProxy";
	static final String INMEMORYPROXY_AUTH = "InMemoryProxy";
	
	// irods extra attributes
	static final String IRODS_USERPASS_AUTH = "UserPassIRODS";
	static final String IRODS_RESOURCE = "Resource"; // "DefaultResource";
	static final String IRODS_ZONE = "Zone"; // "tempZone";
	
	static final String PROXYTYPE_CTX_ATTR = "ProxyType";
	static final String PROXYTYPE_CTX_ATTR_VALUE_GLOBUS = "GLOBUS";
	static final String PROXYTYPE_CTX_ATTR_VALUE_GLOBUSLEGACY = "OLD";
	static final String PROXYTYPE_CTX_ATTR_VALUE_RFC3820 = "RFC3820";
	
	//private final Set<String> legalContextTypes = new HashSet<String> (); // set of context TYPE strings legal to be used in jSAGA ("Globus", "GlobusRFC820, etc.)
	private static final String contextsRequiringCertRepository [] = new String [] {GLOBUS_AUTH, GLOBUSRFC3820_AUTH, GLOBUSLEGACY_AUTH, VOMS_AUTH, MYPROXY_AUTH, INMEMORYPROXY_AUTH, VOMSMYPROXY_AUTH}; // they require CERTREPOSITORY attribute in context
	private static final List<String> contextsRequiringVomsDirAttribute = Arrays.asList(VOMS_AUTH, VOMSMYPROXY_AUTH);
	
	private final String attributesToFileize [] = new String [] {Context.USERKEY, Context.USERCERT, Context.USERPROXY, VOMSContext.INITIALPROXY, "UserPrivateKey" /*0.9.17*/}; // they are required to be files by jSAGA 
	// NOTE: using in memory proxy has problems with jar conficts (org.bouncycastle)
	private boolean USE_IN_MEMORY_PROXY = true; // webapp cannot be undeployed, restarted (Could not load org.bouncycastle.jce.provider.symmetric.AES$ECB.)
	private boolean DELETE_TEMP_FILES = true; // delete temp files (cert/proxy) from tomcat/temp
	
	private final String tempDir; // directory where files created for certs and keys are stored temporarily
	private final static SecureRandom random = new SecureRandom(); // used to generate unique filenames 
	
	// constructor
	SecurityContextHelper (final List<String> supportedProtocols) {
		String sysTemp = System.getProperty("java.io.tmpdir");
		if (sysTemp != null) tempDir = sysTemp;
		else tempDir = "./";
		
		if (!new File(tempDir).exists()) throw new RuntimeException("Cannot access temp directory: " + tempDir);
		
		log.info("Temp dir for credential attributes: " + tempDir);
		init(supportedProtocols); // determine legal context types
	}
	
	// determine legal context types
	private void init(final List<String> supportedProtocols) {
		
//		log.info("Initializing context types for supported protocols (" + supportedProtocols + ")");
//		AdaptorDescriptors adaptorsDescriptorTable;
//		try { adaptorsDescriptorTable = AdaptorDescriptors.getInstance(); } 
//		catch (ConfigurationException e) { log.error("Can't get jSaga adaptor descriptors", e);	return;	}
//		DataAdaptorDescriptor dataAdaptorsDescriptorTable = adaptorsDescriptorTable.getDataDesc();
//
//		Protocol[] dataAdaptorsProtocols = dataAdaptorsDescriptorTable.getXML();
//		for (Protocol dataAdaptorProtocol: dataAdaptorsProtocols) {
//			if (!supportedProtocols.contains(dataAdaptorProtocol.getType())) continue;
//			String[] contextNames = dataAdaptorProtocol.getSupportedContextType();
//			for (String contextName: contextNames) legalContextTypes.add(contextName);
//		}
	}

	// create new jSAGA session object
	Session createNewJSagaSession() throws CredentialException {
        Session jSagaSession;
		try { jSagaSession = SessionFactory.createSession(false); } // false: create new session (without use of default values)
		catch (NoSuccessException e) { throw new CredentialException("Application error: Cannot create jSAGA session" + "\n(" + e.getCause() + ")", e); } // it should not happen due to false parameter @ createSession
		return jSagaSession;
	}

	
	void addContextToSession(final URIBase uri, final Credentials credentials, final Session session) throws URIException, CredentialException {
		if (uri == null || credentials == null || session == null) throw new IllegalArgumentException("null");

		String protocol = uri.getProtocol();
		String host = uri.getHost();
		
		if (protocol == null || host == null) throw new URIException("Unspecified protocol or host!");
		
		Context ctx = createContextFromCredentials(credentials);
		
		// create temp files for credential attributes (it changes credential data)
		convertContextAttributesToTempFiles(ctx); // Context.USERKEY, Context.USERCERT 
        
		boolean generateProxyFromCert = false;
		try { 
			generateProxyFromCert = ctx.existsAttribute(Context.USERCERT);
			if (generateProxyFromCert) log.debug("Create proxy from usercert/userkey (UserCert={})", ctx.getAttribute(Context.USERCERT));
		} catch (Exception e) {}
		
		if (USE_IN_MEMORY_PROXY && !generateProxyFromCert) {
			// create in-memory proxy object from Context.USERPROXY attribute (if there is), but not if it will be generated from cert
			convertUserProxyToUserProxyObject(ctx);
		}
		
		if (generateProxyFromCert) { // this applies to VOMS
			
			// ! DON'T create a dummy, non-existing filename otherwise you get invalid buffer exception!
        	// create dummy temp file for the extended proxy file to be generated
        	createDummyUserProxyAttribute(ctx);
			
//			try { // JSAGA 0.9.17 requires "old" proxy when using certs,  sine 1.1.0 ALL legacy proxies requires "old"
//				ctx.setAttribute(PROXYTYPE_CTX_ATTR, PROXYTYPE_CTX_ATTR_VALUE_GLOBUSLEGACY); //  "old"
//				log.debug(PROXYTYPE_CTX_ATTR + " set to " + PROXYTYPE_CTX_ATTR_VALUE_GLOBUSLEGACY);
//			} catch (Exception e) { log.warn("Cannot set context proxy type", e); }
		}
		
		// set certs dir
		setCertRepositoryIfNeeded(ctx); 

        // disable StrictHostKeyChecking in case of "sftp"
		if (SFTP_PROTOCOL.equals(protocol)) disableSFTPStrictHostKeyCheckingOnSsh(protocol, ctx);
		
		// set irods specific parameters
		if (IRODS_PROTOCOL.equals(protocol)) {
			try {
				// read iRods extra credentials
				String resource = credentials.getCredentialAttribute(IRODS_RESOURCE);
				String zone = credentials.getCredentialAttribute(IRODS_ZONE);
				
				if (resource == null) {
					log.error("Missing IRODS attribute: " + IRODS_RESOURCE);
					resource = "";
				}
				if (zone == null) { 
					log.error("Missing IRODS attribute: " + IRODS_ZONE);
					zone = "";
				}
				log.debug("Using resource: '" + resource + "'");
				log.debug("Using zone: '" + zone + "'");
				ctx.setVectorAttribute("DataServiceAttributes", new String[] {"irods.DefaultResource=" +  resource, "irods.Zone=" + zone});
			} catch (Exception e) {	e.printStackTrace(); } 
		}
		
		// clear previous context registered for this host
		if (session instanceof SessionImpl) {
			SessionImpl impl = (SessionImpl) session;
			URL url = URIImpl.toJSagaURL(protocol + "://" + host);
			Context prev = null; 
			try { prev = impl.findContext(url);	} 
			catch (Exception e) { log.warn("Cannot query context from jSAGA session", e); }
			if (prev != null) {
				log.trace("Removing previous context stored for this host (" + protocol + "://" + host + ")");
				try { session.removeContext(prev); }
				catch (DoesNotExistException e) {} // should not happen
			} else {
				log.trace("No previous context for host: " + protocol + "://" + host + "");
			}
		}
		
        // store context specific for this protocol-host
		log.trace("Making jSAGA context host-specific (" + protocol + "://" + host + ")");
        try { ctx.setVectorAttribute("BaseUrlIncludes", new String[] {protocol + "://" + host}); } // @See: https://forge.in2p3.fr/boards/11/topics/456
        catch (NoSuccessException e) { throw new CredentialException("Invalid context attribute: BaseUrlIncludes", e); }
        catch (TimeoutException e) { throw new CredentialException("TimeoutException", e); }
		catch (DoesNotExistException e) {	throw new CredentialException("Invalid attribute BaseUrlIncludes", e); }
		catch (BadParameterException e) { throw new CredentialException("BadParameterException", e); }
		catch (NotImplementedException e) { throw new CredentialException("NotImplementedException", e);	} 
		catch (AuthenticationFailedException e) { throw new CredentialException("AuthenticationFailedException", e); } 
		catch (AuthorizationFailedException e) { throw new CredentialException("AuthorizationFailedException", e); } 
		catch (PermissionDeniedException e) { throw new CredentialException("PermissionDeniedException", e); } 
        catch (IncorrectStateException e) { throw new CredentialException("IncorrectStateException", e); } 
        
        // if VOMS + InitialUserProxy set, create temp file for generated (VOMS extended) proxy file
        boolean vomsInitialUserProxy = false;
        if (credentials.getCredentialAttribute(VOMSContext.INITIALPROXY) != null) {
        	log.trace("Context type: VOMS/" + VOMSContext.INITIALPROXY + "");
        	// UserProxy must NOT present in this case
        	if (credentials.getCredentialAttribute(Context.USERPROXY) != null) throw new CredentialException("Illegal extra attribute: " + Context.USERPROXY);
        	vomsInitialUserProxy = true;
        	// create dummy temp file for the extended proxy file to be generated
        	createDummyUserProxyAttribute(ctx); 
        }
        
		try { 
			log.trace("Adding jSAGA context to jSAGA session...");
			session.addContext(ctx);
			log.debug("Context added");
		} 
		catch (NoSuccessException e) { // maybe" Unexpected certificate type: "class org.bouncycastle.jce.provider.X509CertificateObject") => Tomcat restart required
			e.printStackTrace();
			log.warn("Authentication failed!", e);
			throw new CredentialException("Authentication failed!", e); 
		}  
		catch (TimeoutException e) { throw new CredentialException("Authentication timeout!", e); }
		finally {
			if (vomsInitialUserProxy || generateProxyFromCert) deleteDummyUserProxyAttribute(ctx);  // delete generated proxy files
			cleanupContextAttributesTempFiles(ctx); // delete temp files for credential attributes (@See: convertContextAttributesToTempFiles())  
		}
	}

	@SuppressWarnings("unused")
	private void printContext(Context ctx) {
		log.trace("Context:");
		try {
			for (String attr: ctx.listAttributes()) {
				try {
					String val = ctx.getAttribute(attr);
					if (val == null) {
						log.trace("  " + attr + "=null"); 
					} else {
						if (attr.equals(Context.USERPASS)/* || val.equals(Context.USERKEY) || val.equals(Context.USERCERT)*/) val = "***";
						if (val.length() > 10) val = val.substring(0, 10) + "...";
						log.trace("  " + attr + "=" + val); 
					}
				}
				catch (Exception e1) { 
					try {
						log.trace("  " + attr + "=" + Arrays.toString(ctx.getVectorAttribute(attr)));
					} catch (Exception e2) { log.trace("Cannot list context attributes: " + e2.getMessage()); }
				}
			}
		} catch (Exception e3) { log.trace("Cannot list context attributes: " + e3.getMessage()); }
	}
	
	private Context createContextFromCredentials(final Credentials credentials) throws CredentialException {
		log.debug("Creating jSAGA context from credential information");

		// change UserPassIRODS to simple UserPass
		if (IRODS_USERPASS_AUTH.equals(credentials.getCredentialAttribute(CredentialsConstants.TYPE/*Context.TYPE*/))) {
			log.debug("Chaning context type from {} to {}", IRODS_USERPASS_AUTH, USERPASS_AUTH);
			credentials.putCredentialAttribute(CredentialsConstants.TYPE/*Context.TYPE*/, USERPASS_AUTH);
		}
		
		String type = credentials.getCredentialAttribute(CredentialsConstants.TYPE/*Context.TYPE*/);
		log.trace("Context type: " + type + "]");
		
		String proxyType = null;
		// try to determine UserProxy Globus proxy type
		if (JSagaGenericAdaptor.AUTO_DETECT_GLOBUS_PROXY && GLOBUS_AUTH.equals(type)) {
			// try to determine Globus proxy type and change to appropriate version if necessary
			String proxyString = credentials.getCredentialAttribute(Context.USERPROXY);
			if (proxyString != null) {
				log.trace("Trying to determine proxy type...");
				log.trace("Proxy: '" + (proxyString.length() > 25 ? proxyString.substring(0, 25) + "..." : proxyString) + "'");
				try {
					byte[] proxyBytes = proxyString.getBytes();
					GSSManager manager = ExtendedGSSManager.getInstance();
					if (manager != null && manager instanceof ExtendedGSSManager) {
						GSSCredential gssCred = ((ExtendedGSSManager)manager).createCredential(proxyBytes, 0, 0, null, 0);
						if (gssCred != null && gssCred instanceof GlobusGSSCredentialImpl) {
							X509Credential globusProxy = ((GlobusGSSCredentialImpl)gssCred).getX509Credential();
							if (globusProxy != null) {
								if(ProxyCertificateUtil.isGsi3Proxy(globusProxy.getProxyType())) {
									log.debug("This is a Globus proxy");
									
									proxyType = GLOBUS_AUTH; // new
									
								} else if(ProxyCertificateUtil.isGsi2Proxy(globusProxy.getProxyType())) {
									log.debug("This is a GlobusLegacy proxy");
//									log.trace("Auto-correcting context type to GlobusLegacy...");
									
//									credentials.putCredentialAttribute(Context.TYPE, GLOBUSLEGACY_AUTH); // just set to "old" 
//									type = credentials.getCredentialAttribute(Context.TYPE);
									
									proxyType = GLOBUSLEGACY_AUTH;
									
								} else if(ProxyCertificateUtil.isGsi4Proxy(globusProxy.getProxyType())) {
									log.debug("Proxy is a GlobusRFC3820 proxy");
//									log.trace("(Leaving proxy type unchanged...)");
//									log.trace("Auto-correcting context type to GlobusRFC3820...");
									
//									credentials.putCredentialAttribute(Context.TYPE, GLOBUSRFC820_AUTH); // new 
//									type = credentials.getCredentialAttribute(Context.TYPE);
									
									proxyType = GLOBUSRFC3820_AUTH;
									
								} else {
									log.warn("Proxy type is unknown (no change in credentials)");
								}
							} else log.warn("Variable globusProxy is null");	
						} else log.warn("Variable gssCred is not an instance of GlobusGSSCredentialImpl");
					} else log.warn("Variable manager is not an instance of ExtendedGSSManager");
				} catch (GSSException e) { log.warn("GSS exception", e); }  
				catch (Throwable e) { log.warn("Unexpected exception", e); }
			}
		}
		
		Context ctx;
		try { ctx = ContextFactory.createContext(type);	} 
		catch (IncorrectStateException e) { throw new CredentialException(e); } 
		catch (TimeoutException e) { throw new CredentialException(e); } 
		catch (NoSuccessException e) { throw new CredentialException(e); }
		
		if (GLOBUS_AUTH.equals(proxyType)) {
			try { ctx.setAttribute(PROXYTYPE_CTX_ATTR, PROXYTYPE_CTX_ATTR_VALUE_GLOBUS); } 
			catch (Exception e) { log.error("Cannot set " + PROXYTYPE_CTX_ATTR + " context attribute (" + PROXYTYPE_CTX_ATTR_VALUE_GLOBUS + ")"); }
		} else if (GLOBUSLEGACY_AUTH.equals(proxyType)) {
			try { ctx.setAttribute(PROXYTYPE_CTX_ATTR, PROXYTYPE_CTX_ATTR_VALUE_GLOBUSLEGACY); } 
			catch (Exception e) { log.error("Cannot set " + PROXYTYPE_CTX_ATTR + " context attribute (" + PROXYTYPE_CTX_ATTR_VALUE_GLOBUSLEGACY + ")"); }
		} else if (GLOBUSRFC3820_AUTH.equals(proxyType)) {
			try { ctx.setAttribute(PROXYTYPE_CTX_ATTR, PROXYTYPE_CTX_ATTR_VALUE_RFC3820); } 
			catch (Exception e) { log.error("Cannot set " + PROXYTYPE_CTX_ATTR + " context attribute (" + PROXYTYPE_CTX_ATTR_VALUE_RFC3820 + ")"); }
		}
		
		for (String key: credentials.keySet()) {
			if (Context.TYPE.equals(key)) {
//				log.trace("Skipping key: " + Context.TYPE);
				continue; // NOTE: setting Context.TYPE attribute again may overwrite other attributes (e.g., UserPass overwrites Context.USERID)
			}
			try {
//				log.trace("Reading key: " + key);
				String value = credentials.getCredentialAttribute(key);
				ctx.setAttribute(key, value);
				// just to print key-value
				value = (value == null) ? "null" : (value.length() > 25 ? value.substring(0, 25) + "..." : value); 
				if (Context.USERPASS.equals(key) || "MyProxyPass".equals(key)) log.trace("context attr: [" + key + "=***]");
				else log.trace("context attr: [" + key + "=" + value + "]");
			} 
			catch (DoesNotExistException e) {	throw new CredentialException("Invalid attribute '" + key + "' in context " + type, e); }
			catch (BadParameterException e) { throw new CredentialException("BadParameterException", e); }
			catch (NotImplementedException e) { throw new CredentialException("NotImplementedException", e);	} 
			catch (AuthenticationFailedException e) { throw new CredentialException("AuthenticationFailedException", e); } 
			catch (AuthorizationFailedException e) { throw new CredentialException("AuthorizationFailedException", e); } 
			catch (PermissionDeniedException e) { throw new CredentialException("PermissionDeniedException", e);	} 
			catch (TimeoutException e) { throw new CredentialException("TimeoutException", e); } 
			catch (NoSuccessException e) { throw new CredentialException("NoSuccessException", e); }
			catch (IncorrectStateException e) { throw new CredentialException("IncorrectStateException", e); }
		}

		if (contextsRequiringVomsDirAttribute.contains(type)) { // set voms dir
			boolean attributeExists = false;
			try { attributeExists = ctx.existsAttribute(VOMSContext.VOMSDIR); } catch (Exception e) { /* does not exist */ }
			
			try { 
				if (attributeExists) {
					String vomsDir = ctx.getAttribute(VOMSContext.VOMSDIR);
					if (!Configuration.vomsDirectory.equals(vomsDir)) {
						log.debug("Replacing attribute '" + VOMSContext.VOMSDIR + "': " + vomsDir + "' -> '" + Configuration.vomsDirectory + "'");
						ctx.setAttribute(VOMSContext.VOMSDIR, Configuration.vomsDirectory);		
					}
				} else {
					log.debug("Setting attribute '" + VOMSContext.VOMSDIR + "' to '" + Configuration.vomsDirectory + "'");
					ctx.setAttribute(VOMSContext.VOMSDIR, Configuration.vomsDirectory);
				}
				
			} catch (Exception e) { log.warn("Cannot get/set VomsDir context attribute!"); }
		}

		return ctx;
	}
	
	/*private void createTempFileForGeneratedProxyFromCert(final Context ctx) throws CredentialException {
		String key = Context.USERPROXY;
		File temp;
		try { temp = File.createTempFile("cred_", "." + key); } 
		catch (IllegalArgumentException e) { throw new CredentialException("Cannot create or write temp files for credential attribute: " + key, e); } // this should not happen
		catch (IOException e) { throw new CredentialException("Cannot create or write temp files for credential attribute: " + key, e); }
		catch (SecurityException e) { throw new CredentialException("Cannot create or write temp files for credential attribute: " + key, e); }
		
		try { temp.deleteOnExit(); } // if delete fails when context added to session, file deleted on jvm exit
		catch (Exception e) {} // silently ignore exceptions (we try to delete this file before JVM exits anyway)
		
		String path = temp.getAbsolutePath();
		log.debug("Adding key " + key + " with value " + path);
		
		try { if (ctx.existsAttribute(key)) log.warn("Context attribute " + key + " exists with value " + ctx.getAttribute(key) + ". It will be overwritten..."); } catch (Exception e) {}
		
		try { 
			ctx.setAttribute(key, path); 
		} // replace String content with file path  
		catch (Exception e) { log.error("Cannot set attribute: " + key); }
	}*/
	
	/*private void removeProxyCreatedFromCert(final Context ctx) throws CredentialException {
		
		if (!DELETE_TEMP_FILES) return;
		
		String key = Context.USERPROXY;
		boolean attributeExists = false;
		try { attributeExists = ctx.existsAttribute(key); } 
		catch (Exception e) { log.warn("Cannot query attribute's existence: " + key); }

		if (!attributeExists) {
			log.warn("Cannot remove attribute: " + key + ", it does not exist...");
			return;
		}
	
		String value;
		try { value = ctx.getAttribute(key); } 
		catch (Exception e) { log.warn("Cannot get attribute: " + key); return; } // it should not happen (<= exists)

		File temp = new File(value);
		log.trace("Deleting temp file: " + temp);
		boolean result = temp.delete();
		if (!result) {
			System.gc(); // @See: http://stackoverflow.com/questions/2128537/how-to-diagnose-file-delete-returning-false-find-unclosed-streams
			result = temp.delete(); 
			if (!result) log.warn("Cannot find or delete temp file created for context attribute " + key);
		}
	}*/

	private void convertContextAttributesToTempFiles(final Context ctx) throws CredentialException {
		log.trace("Converting context attributes to temp files");
		
		for (String key : attributesToFileize) {
			
			if (USE_IN_MEMORY_PROXY && key.equals(Context.USERPROXY)) continue; // don't file-ize user proxy if in-memory-proxy 
			
			boolean attributeExists = false;
			try { attributeExists = ctx.existsAttribute(key); } 
			catch (Exception e) { /* does not exist */ }
			
			if (!attributeExists) continue;
					
			String value;
			try { value = ctx.getAttribute(key); } 
			catch (Exception e) { log.warn("Cannot get attribute: " + key); continue; }
					
			File temp;
			try { temp = File.createTempFile("cred_", "." + key.toString()); } 
			catch (IllegalArgumentException e) { throw new CredentialException("Cannot create or write temp files for credential attribute: " + key, e); } // this should not happen
			catch (IOException e) { throw new CredentialException("Cannot create or write temp files for credential attribute: " + key, e); }
			catch (SecurityException e) { throw new CredentialException("Cannot create or write temp files for credential attribute: " + key, e); }
					
			try { temp.deleteOnExit(); } // if delete fails when context added to session, file deleted on jvm exit
			catch (Exception e) {} // silently ignore exceptions (we try to delete this file before JVM exits anyway)

			String path = temp.getAbsolutePath();
			
			if (VOMSContext.INITIALPROXY.equals(key) ) {
				log.debug("Changing file permissions to rw- --- --- (" + path + ")");
				try {
					temp.setReadable(false, false);
					temp.setWritable(false, false);
					temp.setExecutable(false, false);
					temp.setReadable(true, true);
					temp.setWritable(true, true);
					temp.setExecutable(false, false);
			    } catch (Throwable e) {	log.error("Cannot change file permissions: " + e.getMessage());  }
				
//				try {
//			        Class<?> fspClass = Class.forName("java.util.prefs.FileSystemPreferences");
//			        java.lang.reflect.Method chmodMethod = fspClass.getDeclaredMethod("chmod", String.class, Integer.TYPE);
//			        chmodMethod.setAccessible(true);
//			        log.debug((String)chmodMethod.invoke(null, path, 600));
//			    } catch (Throwable e) {	log.error("Cannot change file permissions: " + e.getMessage());  }
			} 
			
			BufferedWriter bw;
			try { bw = new BufferedWriter(new FileWriter(temp)); }
			catch (IOException e) { throw new CredentialException("Cannot create or write temp files for credential attribute: " + key, e); }
					
			try { bw.write(value); } 
			catch (IOException e) { log.warn("Cannot write temp file for attribute: " + key); }
			finally { try { bw.close(); } catch (IOException e) {} }
					
			log.debug("Replacing key '" + key + "' with value '" + path + "' (containing previous attribute content)");
			
			if (Context.USERCERT.equals(key) | Context.USERKEY.equals(key)) {
				log.debug("Changing file permissions to r-- --- --- (" + path + ")");
				try {
					temp.setReadable(false, false);
					temp.setWritable(false, false);
					temp.setExecutable(false, false);
					temp.setReadable(true, true);
					temp.setWritable(false, true);
					temp.setExecutable(false, false);
			    } catch (Throwable e) {	log.error("Cannot change file permissions: " + e.getMessage());  }
			}
			
			try { ctx.setAttribute(key, path); } // replace String content with file path  
			catch (Exception e) { log.warn("Cannot set attribute: " + key); }
		}
	}
	
	private void createDummyUserProxyAttribute(final Context ctx) throws CredentialException {
		String key = Context.USERPROXY;
//		log.trace("Creating dummy file name for '" + key + "' context attribute...");

		// setting UserProxy to an existing file throws GSS buffer exception
//		File temp;
//		try { temp = File.createTempFile("cred_", "." + key.toString()); } 
//		catch (IllegalArgumentException e) { throw new CredentialException("Cannot create or write temp files for credential attribute: " + key, e); } // this should not happen
//		catch (IOException e) { throw new CredentialException("Cannot create or write temp files for credential attribute: " + key, e); }
//		catch (SecurityException e) { throw new CredentialException("Cannot create or write temp files for credential attribute: " + key, e); }
//		try { temp.deleteOnExit(); } // if delete fails when context added to session, file deleted on jvm exit
//		catch (Exception e) {} // silently ignore exceptions (we try to delete this file before JVM exits anyway)
//		String path = temp.getAbsolutePath();

		long n = random.nextLong();
        n = (n == Long.MIN_VALUE) ? 0 : Math.abs(n);
        String path = tempDir + "/" +  "cred_" + Long.toString(n) + "." + key;
		
		log.debug("Adding dummy filename attribute: " + key + "=" + path + "");

		try { ctx.setAttribute(key, path); }   
		catch (Exception e) { throw new CredentialException("Cannot set '" + key + "' attribute"); }
	}
	
	private void deleteDummyUserProxyAttribute(final Context ctx) {
		if (!DELETE_TEMP_FILES) return;

		String key = Context.USERPROXY;
//		log.trace("Cleaning up temp file created for " + key + " attribute");
		try { 
			if (!ctx.existsAttribute(Context.USERPROXY)) { 
				log.warn("Cannot query attribute's existence: " + key);	
				return;	
			}
		} catch (Exception e) {	log.warn("Cannot query attribute's existence: " + key); return; }
		
		String value;
		try { value = ctx.getAttribute(key); } 
		catch (Exception e) { log.warn("Cannot get attribute: " + key); return; }

		try { ctx.removeAttribute(key); } 
		catch (Exception e) { log.warn("Cannot remove attribute: " + key); }
		
		File temp = new File(value);
		log.trace("Deleting temp file (" + key + "): " + temp);
		boolean result = temp.delete();
		if (!result) {
			System.gc(); // @See: http://stackoverflow.com/questions/2128537/how-to-diagnose-file-delete-returning-false-find-unclosed-streams
			result = temp.delete(); 
			if (!result) log.warn("Cannot find or delete temp file created for context attribute " + key); 
		}
	}
	
	private void cleanupContextAttributesTempFiles(final Context ctx) {
		
		if (!DELETE_TEMP_FILES) return;
		
//		log.trace("Cleaning up temp files created for context attributes");
		
		for (String key: attributesToFileize) {

//			if (key.equals(Context.USERPROXY)) continue; // don't delete user proxy used during connecting later: NoSuccess: GSSException: Failure unspecified at GSS-API level (Mechanism level: [JGLOBUS-63] Invalid buffer)
			if (USE_IN_MEMORY_PROXY && key.equals(Context.USERPROXY)) continue; // don't file-ize user proxy if in-memory-proxy 
			
			boolean attributeExists = false;
			try { attributeExists = ctx.existsAttribute(key); } 
			catch (Exception e) { log.warn("Cannot query attribute's existence: " + key); }
		
			if (!attributeExists) continue;
			
			String value;
			try { value = ctx.getAttribute(key); } 
			catch (Exception e) { log.warn("Cannot get attribute: " + key); continue; }
	
			File temp = new File(value);
			log.trace("Deleting temp file (" + key + "): " + temp);
			boolean result = temp.delete();
			if (!result) {
				System.gc(); // @See: http://stackoverflow.com/questions/2128537/how-to-diagnose-file-delete-returning-false-find-unclosed-streams
				result = temp.delete(); 
				if (!result) log.warn("Cannot find or delete temp file created for context attribute " + key); 
			}
		}
	}
	
	private void setCertRepositoryIfNeeded(final Context ctx) {
		String contextType = null;
		try { contextType  = ctx.getAttribute(Context.TYPE); } catch (Exception e) { log.error("Cannot get context attribute: " + Context.TYPE); }
		
		if (contextType == null) return; 
		
		if (Arrays.asList(contextsRequiringCertRepository).contains(contextType)) {
			
			boolean attributeExists = false;
			try { attributeExists = ctx.existsAttribute(Context.CERTREPOSITORY); } catch (Exception e) { /* does not exist */ }

			try {
				if (attributeExists) {
					String certRepo = ctx.getAttribute(Context.CERTREPOSITORY);
					if (!Configuration.certificatesDirectory.equals(certRepo)) {
						log.debug("Replacing attribute '" + Context.CERTREPOSITORY + "': " + certRepo + "' -> '" + Configuration.certificatesDirectory + "'");
						ctx.setAttribute(Context.CERTREPOSITORY, Configuration.certificatesDirectory);		
					}
				} else {
					log.debug("Setting attribute '" + Context.CERTREPOSITORY + "' to '" + Configuration.certificatesDirectory + "'");
					ctx.setAttribute(Context.CERTREPOSITORY, Configuration.certificatesDirectory);
				}
			} catch (Exception e) { log.error("Cannot get/set attibute: " + Context.CERTREPOSITORY); }
		}
	}
	
	private void disableSFTPStrictHostKeyCheckingOnSsh(final String protocol, final Context ctx) {

		/*String contextType = null;
		try { contextType  = ctx.getAttribute(Context.TYPE); } 
		catch (Exception e) { log.error("Cannot get context attribute: " + Context.TYPE); }
		log.debug("Context type: " + contextType + " (is " + SSH_CONTEXT + "? " + SSH_CONTEXT.equals(contextType) + ")");
		if (contextType == null || !SSH_CONTEXT.equals(contextType)) return; // sftp with UserPass requires it too, StrictHostKeyChecking is for sftp */ 
		
		log.trace("Disabling strict host key checking on SSH");
		
		try {
			// set known_hosts file to null
			ctx.setVectorAttribute("DataServiceAttributes", new String[]{"sftp.KnownHosts="});
			// Note: in earlier jSAGA versions: sftp://?KnownHosts
			log.debug("SSH StrictHostKeyChecking disabled");
		} catch (Exception e) { log.warn("Cannot disable SFTP StrictHostKeyChecking");	}
	}
	
	private void convertUserProxyToUserProxyObject(final Context ctx) {
		if (ctx == null) throw new IllegalArgumentException("null");
		try {
			if (ctx.getAttribute(Context.USERPROXY) != null) {
				String proxy = ctx.getAttribute(Context.USERPROXY);
				ctx.removeAttribute(Context.USERPROXY);
				
				// source: https://forge.in2p3.fr/boards/11/topics/458?r=465
				ExtendedGSSManager manager = (ExtendedGSSManager) ExtendedGSSManager.getInstance();
				GSSCredential cred = manager.createCredential(
				                proxy.getBytes(),
				                ExtendedGSSCredential.IMPEXP_OPAQUE,
				                GSSCredential.DEFAULT_LIFETIME,
				                null, // use default mechanism: GSI
				                GSSCredential.INITIATE_AND_ACCEPT);
		
				// code to pass the GSSCredential object to JSAGA
				ctx.setAttribute("UserProxyObject", fr.in2p3.jsaga.adaptor.security.impl.InMemoryProxySecurityCredential.toBase64(cred));
			}
		} // silently ignore exceptions (no userproxy)
        catch (NoSuccessException e) {}
        catch (TimeoutException e) {}
		catch (DoesNotExistException e) {}
		catch (BadParameterException e) {}
		catch (NotImplementedException e) {} 
		catch (AuthenticationFailedException e) {} 
		catch (AuthorizationFailedException e) {} 
		catch (PermissionDeniedException e) {} 
        catch (IncorrectStateException e) {} 
		catch (GSSException e) {} 
		catch (NullPointerException e) {} // ctx.getAttribute(Context.USERPROXY) throws NPE if no userproxy
	}

    @SuppressWarnings("unused")
	private void setPermissions(File temp) { // 400
    	try {
			temp.setReadable(false, false);
			temp.setWritable(false, false);
			temp.setExecutable(false, false);
			temp.setReadable(true, true);
			temp.setWritable(false, true);
			temp.setExecutable(false, false);
	    } catch (Throwable e) {	log.error("Cannot change file permissions: " + e.getMessage());  }
    }
}