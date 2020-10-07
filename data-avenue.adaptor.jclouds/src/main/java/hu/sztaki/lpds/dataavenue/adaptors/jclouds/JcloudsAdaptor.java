package hu.sztaki.lpds.dataavenue.adaptors.jclouds;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.jclouds.ContextBuilder;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.Apis;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.keystone.config.KeystoneProperties;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;
import static org.jclouds.blobstore.options.PutOptions.Builder.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.MediaType;

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
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.*;

public class JcloudsAdaptor implements Adaptor {
	
	private static final Logger log = LoggerFactory.getLogger(JcloudsAdaptor.class);
	private String adaptorVersion = "1.0.0"; // default adaptor version
	static final String PROTOCOL_PREFIX = "";
	static final List<String> PROTOCOLS = new Vector<String>(); //  = { "aws-s3", "azureblob", "hpcloud-objectstorage", "ninefold-storage", "cloudfiles-uk", "cloudfiles-us" };
	static final List<String> APIS = new Vector<String>();
	static final List<String> PROVIDERS = new Vector<String>();
	public static final String JCLOUDS_SESSION = "jclouds";
	static final String NONE_AUTH = "None"; 
	static final String USERPASS_AUTH = "UserPass";
	static final String SWIFT_AUTH = "Swift";
	private static final String OPENSTACK_SWIFT = "openstack-swift", GOOGLE_CLOUD_STORAGE = "google-cloud-storage", AZURE_BLOB = "azureblob";
	private static final String SWIFT = "swift";
	private static final String AZURE = "azure";
	private static final String GOOGLE = "google";
	public static final String AUTH_PREFIX = "AuthPrefix", KEYSTONE_VERSION = "KeystoneVersion",
			PROJECT_DOMAIN = "ProjectDomain", PROJECT_NAME = "ProjectName", USER_DOMAIN = "UserDomain", 
			USER_ID = "UserID", USER_PASS = "UserPass", HTTP_PROTOCOL = "Protocol";

	public static final long MULTIPART_THRESHOLD = 10000000l;
	
	public JcloudsAdaptor() {
		String PROPERTIES_FILE_NAME = "META-INF/data-avenue-jclouds-adaptor.properties"; // try to read version number
		log.info("Reading properties file "+ PROPERTIES_FILE_NAME);
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
		
		// read APIs
		Iterator<ApiMetadata> ita = Apis.viewableAs(BlobStoreContext.class).iterator();
		while(ita.hasNext()) {
			ApiMetadata e = ita.next();
			String apiId = e.getId();
			log.debug("API: " + apiId);
			if ("filesystem".equalsIgnoreCase(apiId) ||	"transient".equalsIgnoreCase(apiId)) continue; // skip filsystem, memory, etc.
			APIS.add(e.getName());
			PROTOCOLS.add(PROTOCOL_PREFIX + getProtocolShortName(apiId));
		}
		
		// read BlobStoreContext providers
		Iterator<ProviderMetadata> it = Providers.viewableAs(BlobStoreContext.class).iterator();
		while(it.hasNext()) {
			ProviderMetadata e = it.next();
			log.debug("PROTOCOL: " + PROTOCOL_PREFIX + e.getId());
			PROVIDERS.add(e.getName());
			PROTOCOLS.add(PROTOCOL_PREFIX + getProtocolShortName(e.getId())); // provider-specific "protocols"
		}
		
		log.info("JClouds adaptor instantiated");
		log.info("JClouds protocols: " + PROTOCOLS.size());
	}
	
	// return short-name for JClouds APIs and protocols
	private String getProtocolShortName(String provider) {
		if (OPENSTACK_SWIFT.equals(provider)) return SWIFT;
		else if (GOOGLE_CLOUD_STORAGE.equals(provider)) return GOOGLE;
		else if (AZURE_BLOB.equals(provider)) return AZURE;
		else return provider;
	}

	private String getProviderName(String protocol) {
		if (SWIFT.equals(protocol)) return OPENSTACK_SWIFT;
		else if (GOOGLE.equals(protocol)) return GOOGLE_CLOUD_STORAGE;
		else if (AZURE.equals(protocol)) return AZURE_BLOB;
		else return protocol;
	}

	/* adaptor meta information */
	@Override public String getName() { return "Jclouds Adaptor"; }
	@Override public String getDescription() { return "Jclouds Adaptor allows of connecting to cloud storages using Apache jclouds API (providers: " + getCommaSeparatedString(PROVIDERS) + ", apis: " + getCommaSeparatedString(APIS) + ")"; }
	// helper to get comma separated list from List<String>
	private String getCommaSeparatedString(List<String> l) {
		if (l == null || l.size() == 0) return "";
		StringBuilder sb = new StringBuilder();
		for (String s: l) sb.append(s + ", ");
		return "" + sb.toString().substring(0, sb.length() - 2) + "";
	}
	@Override public String getVersion() { return adaptorVersion; }
	
	@Override public List<String> getSupportedProtocols() {
		return Arrays.asList(SWIFT/*, AZURE, GOOGLE*/); 
	}
	
	// list of operations that this adapter can do over that protocol
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
	
	// operations when the same adaptor serves two protocols (not relevant here)
	@Override public List<OperationsEnum> getSupportedOperationTypes(final URIBase fromURI, final URIBase toURI) {
		return Collections.<OperationsEnum>emptyList();
	}
	
	// deprecated, @See getAuthenticationTypeList(String protocol)
	@Override public List<String> getAuthenticationTypes(String protocol) {
		List<String> result = new Vector<String>();
		// for all protocols 
		result.add(USERPASS_AUTH);
		return result;
	}
	
	@Override public AuthenticationTypeList getAuthenticationTypeList(String protocol) {
		if (SWIFT.equals(protocol)) {
			AuthenticationType a = new AuthenticationTypeImpl();
			a.setType(SWIFT_AUTH);
			a.setDisplayName("Swift authentication");
			
			AuthenticationField f;

			f = new AuthenticationFieldImpl();
			f.setDisplayName("Authentication URL prefix (v3|v2|v1) or URL (http://keystone....:5000/v3)");
			f.setKeyName(AUTH_PREFIX);
			f.setDefaultValue("v3");
			a.getFields().add(f);

			f = new AuthenticationFieldImpl();
			f.setDisplayName("Endpoint protocol (http|https)");
			f.setKeyName(HTTP_PROTOCOL);
			f.setDefaultValue("https");
			a.getFields().add(f);
			
			f = new AuthenticationFieldImpl();
			f.setDisplayName("Keystone version (3|2|1)");
			f.setKeyName(KEYSTONE_VERSION);
			f.setDefaultValue("3");
			a.getFields().add(f);
			
			f = new AuthenticationFieldImpl();
			f.setDisplayName("Project domain");
			f.setKeyName(PROJECT_DOMAIN);
			f.setDefaultValue("default");
			a.getFields().add(f);
			
			f = new AuthenticationFieldImpl();
			f.setDisplayName("Project (tenant) name");
			f.setKeyName(PROJECT_NAME);
			a.getFields().add(f);

			f = new AuthenticationFieldImpl();
			f.setDisplayName("User domain");
			f.setKeyName(USER_DOMAIN);
			f.setDefaultValue("default");
			a.getFields().add(f);

			f = new AuthenticationFieldImpl();
			f.setDisplayName("Username");
			f.setKeyName(USER_ID);
			a.getFields().add(f);
			
			f = new AuthenticationFieldImpl();
			f.setDisplayName("Password");
			f.setKeyName(USER_PASS);
			f.setType(AuthenticationField.PASSWORD_TYPE);
			a.getFields().add(f);

			AuthenticationTypeList l = new AuthenticationTypeListImpl();
			l.getAuthenticationTypes().add(a);
			
			return l;
			
		} else if (GOOGLE.equals(protocol) || AZURE.equals(protocol)) {
			
			AuthenticationType a = new AuthenticationTypeImpl();
			a.setType(USERPASS_AUTH);
			if (GOOGLE.equals(protocol)) a.setDisplayName("Google cloud storage authentication");
			else a.setDisplayName("Azure authentication");
			
			AuthenticationField f;

			f = new AuthenticationFieldImpl();
			f.setDisplayName("Username");
			f.setKeyName(USER_ID);
			a.getFields().add(f);
			
			f = new AuthenticationFieldImpl();
			f.setDisplayName("Password");
			f.setKeyName(USER_PASS);
			f.setType(AuthenticationField.PASSWORD_TYPE);
			a.getFields().add(f);

			AuthenticationTypeList l = new AuthenticationTypeListImpl();
			l.getAuthenticationTypes().add(a);
			return l;
		
	 	} else if ("s3".equals(protocol)) { // not handled by this adaptor
	 		
			AuthenticationType a = new AuthenticationTypeImpl();
			a.setType(USERPASS_AUTH);
			a.setDisplayName("S3 authentication");
			
			AuthenticationField f = new AuthenticationFieldImpl();
			f.setKeyName("UserID");
			f.setDisplayName("Access key");
			a.getFields().add(f);
			
			f = new AuthenticationFieldImpl();
			f.setKeyName("UserPass");
			f.setDisplayName("Secret key");
			a.getFields().add(f);
			
			AuthenticationTypeList l = new AuthenticationTypeListImpl();
			l.getAuthenticationTypes().add(a);
			
			return l;
			
		} else { // this should not happen
			
			AuthenticationType a = new AuthenticationTypeImpl();
			a.setType(USERPASS_AUTH);
			a.setDisplayName("Unknown authentication");
			AuthenticationTypeList l = new AuthenticationTypeListImpl();
			l.getAuthenticationTypes().add(a);
			return l;
			
		}
	}	
	
	// deprecated
	@Override public String getAuthenticationTypeUsage(String protocol,
			String authenticationType) {
		if (protocol == null || authenticationType == null) throw new IllegalArgumentException("null argument");
		if (USERPASS_AUTH.equals(authenticationType)) return "<b>UserID</b> (access key), <b>UserPass</b> (secret key)";
		return null;
	}
	

	@Override public List<URIBase> list(final URIBase uri, final Credentials credentials, final DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return list(uri, credentials, session, false, null);
	}

	// get the list of subentries at a given dir (or url) URI
	private List<URIBase> list(final URIBase uri, final Credentials credentials, final DataAvenueSession session, final boolean details, final List <String> subentriesOfInterest) throws URIException, OperationException, CredentialException {

		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a URL or a directory!");

		BlobStoreContext blobStoreContext = getBlobStoreContext(uri, credentials, session);  
		BlobStore blobStore = null;
		try {
			blobStore = blobStoreContext.getBlobStore();

			List<URIBase> result = new Vector<URIBase>();
			PageSet<? extends StorageMetadata> page = null;
			String marker = null; // next page?

			JCloudsURI jcloudsURI = new JCloudsURI(uri);
			String container = jcloudsURI.getContainerName();
			String directory = jcloudsURI.getDirectoryPath(); // dir path relative in container without trailing slash
			do {
				// if this is the root path, list containers (container == null and directory == null)
				if (container == null) {
					log.debug("Listing containers (no container)");
					page = blobStore.list(); 
				} else {
					if (directory == null) { 
						log.debug("Listing contents of container: " + container);
						if (marker == null) page = blobStore.list(container);
						else page = blobStore.list(container, new ListContainerOptions().afterMarker(marker));
					} else {
						log.debug("Listing contents of directory: " + container + "/" + directory);
						if (marker == null) 
							page = blobStore.list(container, new ListContainerOptions().prefix(directory  + JCloudsURI.PATH_SEPARATOR)); // .withDetails() does not return subdirs
						else 
							page = blobStore.list(container, new ListContainerOptions().afterMarker(marker).prefix(directory + JCloudsURI.PATH_SEPARATOR));
					}
				}

				if (page == null) throw new OperationException("page null"); // empty?
				
				Iterator<? extends StorageMetadata> it = page.iterator();
				while (it.hasNext()) {
					StorageMetadata metaData = it.next(); // blob name relative to container, dirs end with / (or of type RELATIVE_PATH)
					log.debug("Listing item: " + metaData.getName() + " " + metaData.getType() + " in path " + container + "/" + directory);
					switch (metaData.getType()) {
						case CONTAINER:
						case FOLDER: 
						case RELATIVE_PATH: {
							String subdirName = metaData.getName();
							if (!subdirName.endsWith(JCloudsURI.PATH_SEPARATOR)) subdirName += URIBase.PATH_SEPARATOR;
							
							if (subentriesOfInterest != null && subentriesOfInterest.size() > 0) { // must use filtering
								// get dir name only (not relative path within container)
								String name = subdirName.substring(0, subdirName.length() - 1); // cut trailing /
								if (name.contains(JCloudsURI.PATH_SEPARATOR)) name = name.substring(name.lastIndexOf(JCloudsURI.PATH_SEPARATOR)); 
								if (!subentriesOfInterest.contains(name)) break; // filter subentry not of interest
							}

							DefaultURIBaseImpl item = new DefaultURIBaseImpl(jcloudsURI.getURI() + subdirName);
							if (details) {
								if (metaData.getSize() != null) item.setSize(metaData.getSize());
								if (metaData.getLastModified() != null) item.setLastModified(metaData.getLastModified().getTime());
							}
							result.add(item);
							break;
						}
						case BLOB:
						default: { 
							String blobName = metaData.getName();
							if (blobName.endsWith(JCloudsURI.PATH_SEPARATOR)) // this is a dir entry, but metadata is incorrectly set to BLOB
								blobName = blobName.substring(0, blobName.length() - 1);
							
							if (subentriesOfInterest != null && subentriesOfInterest.size() > 0) { // must use filtering
								// get file name only
								String name = blobName.contains(JCloudsURI.PATH_SEPARATOR) ? blobName.substring(blobName.lastIndexOf(JCloudsURI.PATH_SEPARATOR)) : blobName; 
								if (!subentriesOfInterest.contains(name)) break; // filter subentry not of interest
							}
							
							if (blobName.equals(directory)) break; // for some reason, JClouds create a blob for subdir (wihout trailing / and of type BLOB), skip it at listing

							DefaultURIBaseImpl item = new DefaultURIBaseImpl(jcloudsURI.getURI() + blobName);
							if (details) {
								if (metaData.getSize() != null) item.setSize(metaData.getSize());
								if (metaData.getLastModified() != null) item.setLastModified(metaData.getLastModified().getTime());
							}
							result.add(item);
						}
					}
				}
				marker = page.getNextMarker();
			} while (marker != null);
			
			return result;
			
		} catch (Throwable e) {
			log.warn("Operation failed!", e);
			throw new OperationException(e);
		}
		finally {
			if (session == null && blobStoreContext != null) blobStoreContext.close();
		}
	}
	
	@Override public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("Directory path expected!");
		URIBase parent = new JCloudsURI(uri).getParent();		
		List<URIBase> attrList = attributes(parent, credentials, session, Arrays.asList(uri.getEntryName()));
		if (attrList.size() == 0) throw new OperationException("Blob not found: " + uri);
		else return attrList.get(0);
	}
	
	@Override public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession session, List <String> subentries) throws URIException, OperationException, CredentialException {
		return list(uri, credentials, session, true, subentries);
	}
	
	@SuppressWarnings("deprecation")
	@Override public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("Directory path expected!");
		BlobStoreContext blobStoreContext = getBlobStoreContext(uri, credentials, session);
		BlobStore blobStore = null;
		try {
			blobStore = blobStoreContext.getBlobStore();
			
			JCloudsURI jcloudsURI = new JCloudsURI(uri);
			String container = jcloudsURI.getContainerName();
			String directory = jcloudsURI.getDirectoryPath(); // without trailing /
	
			if (container == null) throw new OperationException("No container name provided!");
			if (directory == null) { // create new container
				log.debug("Create new container: " + container);
				if (blobStore.containerExists(container)) throw new OperationException("Container " + container + " already exists!");
				blobStore.createContainerInLocation(null, container);
			} else {
				log.debug("Create new directory in container " + container + ": " + directory + JCloudsURI.PATH_SEPARATOR);
				if (blobStore.blobExists(container, directory + JCloudsURI.PATH_SEPARATOR)) throw new OperationException("Directory " + container + "/" + directory + " already exists!");
				blobStore.createDirectory(container, directory + JCloudsURI.PATH_SEPARATOR);
				/*Blob blob = blobStore.blobBuilder(directory + JCloudsURI.PATH_SEPARATOR)
				    .payload(new byte[0])
				    .contentLength(0)
				    .type(StorageType.RELATIVE_PATH)
				    .build();
				blobStore.putBlob(container, blob);*/
			}
		} finally {
			if (session == null && blobStoreContext != null) blobStoreContext.close();
		}
	}

	@SuppressWarnings("deprecation")
	@Override public void rmdir(final URIBase uri, final Credentials credentials, final DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("Not a directory URI!");
		BlobStoreContext blobStoreContext = getBlobStoreContext(uri, credentials, session);
		try {
			BlobStore blobStore = blobStoreContext.getBlobStore();
			JCloudsURI jcloudsURI = new JCloudsURI(uri);
			String container = jcloudsURI.getContainerName();
			String directory = jcloudsURI.getDirectoryPath(); // directory path relative to container without terminating /

			if (container == null) throw new OperationException("No container name provided!");

			if (directory == null) { // delete container
				if (!blobStore.containerExists(container)) throw new OperationException("Container " + container + " does not exist!");
				log.debug("Deleting container: " + container);
				blobStore.clearContainer(container);
				blobStore.deleteContainer(container);
			} else { // delete directory
				if (!blobStore.directoryExists(container, directory + JCloudsURI.PATH_SEPARATOR)) throw new OperationException("Directory " + container + JCloudsURI.PATH_SEPARATOR + directory + JCloudsURI.PATH_SEPARATOR + " does not exist!");
				log.debug("Deleting directory in container " + container + ": " + directory + JCloudsURI.PATH_SEPARATOR);
				deleteDirectoryRecursively(blobStore, container, directory);
//				blobStore.deleteDirectory(container, directory + JCloudsURI.PATH_SEPARATOR); 
			}
		} finally {
			if (session == null && blobStoreContext != null) blobStoreContext.close();
		}
	}
	
	// remove all blobs having directory prefix
	@SuppressWarnings("deprecation")
	private void deleteDirectoryRecursively(BlobStore blobStore, String container, String directory) throws URIException, OperationException, CredentialException {
		log.debug("Removing directory recursively: " + container + "/" + directory);
		PageSet<? extends StorageMetadata> page = null;
		String marker = null; // next page?
		do {
			if (marker == null)	page = blobStore.list(container, new ListContainerOptions().prefix(directory  + JCloudsURI.PATH_SEPARATOR)); 
			else page = blobStore.list(container, new ListContainerOptions().afterMarker(marker).prefix(directory + JCloudsURI.PATH_SEPARATOR));
			if (page == null) return; // empty?
			
			Iterator<? extends StorageMetadata> it = page.iterator();
			while (it.hasNext()) {
				StorageMetadata metaData = it.next(); // blob name relative to container, dirs end with / (or of type RELATIVE_PATH)
				log.debug("Listing item: " + metaData.getName() + " " + metaData.getType() + " in path " + container + "/" + directory);
				switch (metaData.getType()) {
					case FOLDER: 
					case RELATIVE_PATH:
						log.debug("Deleting directory: " + metaData.getName());
						blobStore.deleteDirectory(container, metaData.getName());
						break;
					case BLOB:
					default: 
						log.debug("Deleting blob: " + metaData.getName());
						blobStore.removeBlob(container, metaData.getName());
						break;
					
				}
			}
			marker = page.getNextMarker();
		} while (marker != null);
	}

	@Override public void delete(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("Not a file URI!");

		BlobStoreContext blobStoreContext = getBlobStoreContext(uri, credentials, session);
		try {
			BlobStore blobStore = blobStoreContext.getBlobStore();
			JCloudsURI jcloudsURI = new JCloudsURI(uri);
			String container = jcloudsURI.getContainerName();
			String directory = jcloudsURI.getDirectoryPath();
			String entryName = jcloudsURI.getEntryName(); 
			if (container == null) throw new OperationException("No container provided!");
			if (entryName == null) throw new OperationException("No file name provided!");
			String blobName = directory == null ? entryName : directory + JCloudsURI.PATH_SEPARATOR + entryName;
			if (!blobStore.blobExists(container, blobName)) throw new OperationException("File " + container + JCloudsURI.PATH_SEPARATOR + blobName + " does not exist!");
			log.debug("Deleting: " + blobName);
			blobStore.removeBlob(jcloudsURI.getContainerName(), blobName);
		} finally {
			if (session == null && blobStoreContext != null) blobStoreContext.close();
		}
	}

	@Override public void permissions(final URIBase uri, final Credentials credentials, final DataAvenueSession session, final String permissionsString) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}
	
	@Override public void rename(URIBase uri, String newName, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported on " + uri.getProtocol());
	}

	@Override public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("Not a file URI!");

		BlobStoreContext blobStoreContext = getBlobStoreContext(uri, credentials, session);
		try {
			BlobStore blobStore = blobStoreContext.getBlobStore();
			JCloudsURI jcloudsURI = new JCloudsURI(uri);
			String container = jcloudsURI.getContainerName();
			String directory = jcloudsURI.getDirectoryPath();
			String entryName = jcloudsURI.getEntryName(); 
			if (container == null) throw new OperationException("No container provided!");
			try { 
				String blobName = directory == null ? entryName : directory + JCloudsURI.PATH_SEPARATOR + entryName;
				if (!blobStore.blobExists(container, blobName)) throw new OperationException("File " + container + JCloudsURI.PATH_SEPARATOR + directory + JCloudsURI.PATH_SEPARATOR + entryName + " does not exist!");
				return blobStore.getBlob(container, blobName).getPayload().openStream();
			} catch (IOException x) { 
				throw new OperationException(x);
			}
		} finally {
			if (session == null && blobStoreContext != null) blobStoreContext.close();
		}
	}

	@Override public OutputStream getOutputStream(URIBase uri, Credentials credentials,	DataAvenueSession session, long contentLength) throws URIException, OperationException,	CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("Not a file URI!");

		// maybe an OutputStream wrapper (or two, one for multipart) class can be written to accept bytes written to it and upload parts
		// DA can live without it, as always writeFromInputStream tried first which should succeed
		throw new OperationException("getOutputStream not supported");
		
//		blobStore.initiateMultipartUpload(container, directory + JCloudsURI.PATH_SEPARATOR + entryName, new PutOptions().multipart(true));
//		MultipartUpload mpu = MultipartUpload.create(container, directory + JCloudsURI.PATH_SEPARATOR + entryName, id, null, new PutOptions().multipart(true));
	}
	
	@Override public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session, InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException, OperationNotSupportedException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("Not a file URI!");
		if (contentLength < 0) throw new OperationException("Cannot write data to target storage without size information (" + contentLength + ")");
		BlobStoreContext blobStoreContext = getBlobStoreContext(uri, credentials, session);
		try {
			BlobStore blobStore = blobStoreContext.getBlobStore();
			JCloudsURI jcloudsURI = new JCloudsURI(uri);
			String container = jcloudsURI.getContainerName();
			String directory = jcloudsURI.getDirectoryPath();
			String entryName = jcloudsURI.getEntryName(); 
			if (container == null) throw new OperationException("No container provided!");
			try { 
				String blobName = directory == null ? entryName : directory + JCloudsURI.PATH_SEPARATOR + entryName;
				log.debug("blobname: " + blobName);
				BlobBuilder blobBuilder = blobStore.blobBuilder(blobName)
					.payload(inputStream)
					.contentDisposition("attachment; filename=" + entryName)
					.contentType(MediaType.OCTET_STREAM.toString())
					.contentLength(contentLength)
	//				.contentEncoding(contentEncoding);
	//				.contentLanguage(contentLanguage)
	//				.contentMD5(md5)
					;
				boolean multipart = contentLength > MULTIPART_THRESHOLD; // contentLength > MULTIPART_THRESHOLD
//			String eTag = 
				blobStore.putBlob(container, blobBuilder.build(), multipart(multipart));
			} catch (ContainerNotFoundException x) { 
				throw new OperationException("Container " + container + " does not exist!");
			}
		} finally {
			if (session == null && blobStoreContext != null) blobStoreContext.close();
		}
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
		BlobStoreContext blobStoreContext = null;
		try {
			blobStoreContext = getBlobStoreContext(uri, credentials, session);
			BlobStore blobStore = blobStoreContext.getBlobStore();
			
			JCloudsURI jcloudsURI = new JCloudsURI(uri);
			String container = jcloudsURI.getContainerName();
			String entryName = jcloudsURI.getEntryName(); 

			BlobMetadata meta = blobStore.blobMetadata(container, entryName);
			if (meta != null && meta.getContentMetadata() != null) return meta.getContentMetadata().getContentLength();
			else throw new OperationException("Cannot get file size!");
		} finally {
			if (session == null && blobStoreContext != null) blobStoreContext.close();
		}
	}

	@SuppressWarnings("deprecation")
	public boolean exists(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		BlobStoreContext blobStoreContext = null;
		try {
			blobStoreContext = getBlobStoreContext(uri, credentials, session);
			BlobStore blobStore = blobStoreContext.getBlobStore();

			JCloudsURI jcloudsURI = new JCloudsURI(uri);
			String container = jcloudsURI.getContainerName();
			String directory = jcloudsURI.getDirectoryPath();
			String entryName = jcloudsURI.getEntryName(); 
			
			if (container == null) throw new OperationException("No container name provided!");
			if (directory == null) {
				boolean result = uri.getType() == URIBase.URIType.FILE ? 
						blobStore.blobExists(container, entryName) :
						blobStore.containerExists(container);
				log.debug(uri.getURI() + " " + uri.getType() + " exists: " + result + " (" + container + ", " + entryName + ")");
				return result;
			} else {
				boolean result = uri.getType() == URIBase.URIType.FILE ? 
					blobStore.blobExists(container, directory + JCloudsURI.PATH_SEPARATOR + entryName) :
					blobStore.directoryExists(container, directory + JCloudsURI.PATH_SEPARATOR);
				log.debug(uri.getURI() + " " + uri.getType() + " exists: " + result + " (" + directory + ", " + entryName + ")");
				return result;
			}
		} finally {
			if (session == null && blobStoreContext != null) blobStoreContext.close();
		}
	}
	
	// test if object is readable
	@Override public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return exists(uri, credentials, session);
	}

	@Override public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return !exists(uri, credentials, session);
	}

	@Override public void shutDown() {
		// no resources to free up
	}
	
	private BlobStoreContext getBlobStoreContext(final URIBase uri, final Credentials credentials, final DataAvenueSession session) throws URIException, CredentialException, OperationException {
		JcloudsSession jCloudsSession = (session != null && session.containsKey(JCLOUDS_SESSION)) ? (JcloudsSession) session.get(JCLOUDS_SESSION) : null;
		String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : ""); 
		@SuppressWarnings("resource")
		BlobStoreContext blobStoreContext = jCloudsSession != null ? jCloudsSession.get(hostAndPort) : null;
		if (blobStoreContext == null) {
			blobStoreContext = createBlobStoreContext(uri, credentials);
			if (session != null) {
				if (jCloudsSession == null) jCloudsSession = new JcloudsSession(); 
				jCloudsSession.put(hostAndPort, blobStoreContext);
				session.put(JCLOUDS_SESSION, jCloudsSession);
			}
		}
		return blobStoreContext;
	} 
	
	private BlobStoreContext createBlobStoreContext(final URIBase uri, final Credentials credentials) throws URIException, CredentialException {
		
		if (SWIFT.equals(uri.getProtocol())) {
			
			String keystoneVersion = credentials.optCredentialAttribute(KEYSTONE_VERSION, "3");		
			String projectDomain = credentials.optCredentialAttribute(PROJECT_DOMAIN, "default");
			String projectName = credentials.getCredentialAttribute(PROJECT_NAME);
			log.debug("Creating blobstore context with fields:");
			log.debug("KEYSTONE_VERSION: " + keystoneVersion);
			log.debug("PROJECT_DOMAIN: " + projectDomain);
			log.debug("PROJECT_NAME: " + projectName);
			
			if (projectName == null) throw new CredentialException("Missing credential attribute: " + PROJECT_NAME);
			
			Properties overrides = new Properties();
			overrides.put(KeystoneProperties.KEYSTONE_VERSION, keystoneVersion);
			overrides.put(KeystoneProperties.SCOPE, "domain:" + projectDomain);
			overrides.put(KeystoneProperties.SCOPE, "project:" + projectName);
			overrides.put(KeystoneProperties.CREDENTIAL_TYPE, "apiAccessKeyCredentials"); // FIXME ?
			overrides.put("jclouds.wire.log.sensitive", "True");

			String contextAPI = "openstack-swift";
			String authPrefix = credentials.optCredentialAttribute(AUTH_PREFIX, "");
			String protocol = credentials.optCredentialAttribute(HTTP_PROTOCOL, "https");
			String endpoint = JCloudsURI.getAuthEndpoint(uri, protocol, authPrefix);
			String username = credentials.getCredentialAttribute(USER_ID);
			String userDomain = credentials.optCredentialAttribute(USER_DOMAIN, "default");
			String password = credentials.getCredentialAttribute(USER_PASS);
			
			log.debug("Context API: " + contextAPI);
			log.debug("AUTH_PREFIX: " + authPrefix);
			log.debug("HTTP_PROTOCOL: " + protocol);
			log.debug("Endpoint: " + endpoint);
			log.debug("USER_DOMAIN: " + userDomain);
			log.debug("USER_ID: " + username);

			if (username == null) throw new CredentialException("Missing credential attribute: " + USER_ID);
			if (password == null) throw new CredentialException("Missing credential attribute: " + USER_PASS);
			
			BlobStoreContext blobStoreContext = ContextBuilder.newBuilder(contextAPI)
				.endpoint(endpoint)
				.credentials((!"".equals(userDomain) ? userDomain + ":" : "")+ username, password) // tenant:user
				.overrides(overrides)
				.modules(ImmutableSet.of(new SLF4JLoggingModule()))
				.buildView(BlobStoreContext.class);
			
			return blobStoreContext;
		
		} else if (GOOGLE.equals(uri.getProtocol()) || AZURE.equals(uri.getProtocol())) {
			String username = credentials.getCredentialAttribute(USER_ID);
			String password = credentials.getCredentialAttribute(USER_PASS);
			if (username == null) throw new CredentialException("Missing credential attribute: " + USER_ID);
			if (password == null) throw new CredentialException("Missing credential attribute: " + USER_PASS);
			String provider = getProviderName(uri.getProtocol());
			BlobStoreContext context = ContextBuilder.newBuilder(provider)
		              .credentials(username, password)
		              .buildView(BlobStoreContext.class);
			return context;
		} else {
			throw new URIException("Unknown protocol: " + uri.getProtocol());
		}
	}
}