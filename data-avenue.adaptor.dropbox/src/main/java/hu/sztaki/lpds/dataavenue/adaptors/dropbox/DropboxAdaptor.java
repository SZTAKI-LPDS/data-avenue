package hu.sztaki.lpds.dataavenue.adaptors.dropbox;

import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.DELETE;
import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.INPUT_STREAM;
import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.LIST;
import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.MKDIR;
import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.OUTPUT_STREAM;
import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.PERMISSIONS;
import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.RENAME;
import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.RMDIR;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;

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

public class DropboxAdaptor implements Adaptor {

	private static final Logger log = LoggerFactory.getLogger(DropboxAdaptor.class);

	private String adaptorVersion = "1.0.0"; // default adaptor version

	static final String PROTOCOL_PREFIX = "dropbox";
	static final String PROTOCOLS = "dropbox";
	
//	static final List<String> APIS = new Vector<String>(); 
//	static final List<String> PROVIDERS = new Vector<String>();

//	static final String USERPASS_AUTH = "UserPass";

	static final String ACCESS_KEY_USERNAME = "accessKeyUserName"; // FIXME neve?
	static final String ACCESS_KEY_CREDENTIAL = "accessKey"; // FIXME token credential?
//	static final String LEGACY_ACCESS_KEY_CREDENTIAL = "UserID";

	public DropboxAdaptor() {
		String PROPERTIES_FILE_NAME = "META-INF/data-avenue-adaptor.properties"; // try to read version number
		try {
			Properties prop = new Properties();
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME); // emits no
			// returned
			if (in == null)
				log.warn("Cannot find properties file: " + PROPERTIES_FILE_NAME);
			else {
				try {
					prop.load(in);
					try {
						in.close();
					} catch (IOException e) {
					}
					if (prop.get("version") != null)
						adaptorVersion = (String) prop.get("version");
				} catch (Exception e) {
					log.warn("Cannot load properties from file: " + PROPERTIES_FILE_NAME);
				}
			}
		} catch (Throwable e) {
			log.warn("Cannot read properties file: " + PROPERTIES_FILE_NAME);
		}
	}

	/* adaptor meta information */
	@Override
	public String getName() {
		return "Dropbox Adaptor";
	}

	@Override
	public String getDescription() {
		return "Dropbox Adaptor allows of connecting to Dropbox drive storages";
	}

	@Override
	public String getVersion() {
		return adaptorVersion;
	}

	@Override
	public List<String> getSupportedProtocols() {
		List<String> result = new Vector<String>();
		result.add(PROTOCOLS);
		return result;
	}

	@Override
	public List<OperationsEnum> getSupportedOperationTypes(String protocol) {
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

	@Override
	public List<OperationsEnum> getSupportedOperationTypes(final URIBase fromURI, final URIBase toURI) {
		return getSupportedOperationTypes(PROTOCOL_PREFIX);
	}

	@Override
	public List<String> getAuthenticationTypes(String protocol) {
		List<String> result = new Vector<String>();
		// for all protocols
		result.add(ACCESS_KEY_CREDENTIAL);
		return result;
	}

	@Override
	public String getAuthenticationTypeUsage(String protocol, String authenticationType) {
		if (protocol == null || authenticationType == null)
			throw new IllegalArgumentException("null argument");
		if (ACCESS_KEY_CREDENTIAL.equals(authenticationType))
			return "<b>" + ACCESS_KEY_USERNAME + "</b> (user name) <b>" + ACCESS_KEY_CREDENTIAL + "</b> (access key)"; // FIXME
		return null;
	}

	@Override
	public AuthenticationTypeList getAuthenticationTypeList(String arg0) {

		AuthenticationTypeList l = new AuthenticationTypeListImpl();

		AuthenticationType a = new AuthenticationTypeImpl();

		a = new AuthenticationTypeImpl();
		a.setType(ACCESS_KEY_CREDENTIAL);
		a.setDisplayName("Dropbox credential");

		AuthenticationField f1 = new AuthenticationFieldImpl();
		f1.setKeyName(ACCESS_KEY_CREDENTIAL);
		f1.setDisplayName("Dropbox username"); // FIXME
		a.getFields().add(f1);

		
		AuthenticationField f2 = new AuthenticationFieldImpl();
		f2.setKeyName(ACCESS_KEY_CREDENTIAL);
		f2.setDisplayName("Dropbox token");
		a.getFields().add(f2);

		l.getAuthenticationTypes().add(a);

		return l;
	}

	private DbxClientV2 getDropboxClient(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws OperationException, GeneralSecurityException, CredentialException {
		DropboxClient clients = null; // FIXME
		if (clients == null) {
			if (credentials == null)
				throw new CredentialException("No credentials!");

			String username = credentials.getCredentialAttribute(ACCESS_KEY_USERNAME);
			String accessKey = credentials.getCredentialAttribute(ACCESS_KEY_CREDENTIAL);

			try {
				clients = new DropboxClient().withClient(uri, username, accessKey);
			} catch (IOException x) {
				throw new OperationException(x);
			}
		}
		DbxClientV2 client = clients.get(uri);
		if (client == null)
			throw new OperationException("APPLICATION ERROR: Cannot create Dropbox client!");
		return client;
	}

	@Override
	public List<URIBase> list(final URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException {
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY)
			throw new URIException("URI is not a URL or a directory!");
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			List<URIBase> result = new Vector<URIBase>();

			log.info("dropbox: " + client.auth());
			log.info("dropbox: " + client.fileProperties());
			log.info("dropbox: " + client.users().getCurrentAccount().getName());

			// Get files and folder metadata from Drop box root directory
			log.info("list path: " + uri.getPath());
			ListFolderResult res = client.files().listFolder("/" + uri.getPath());

			while (true) {
				for (Metadata metadata : res.getEntries()) {
					log.info("google drive: " + metadata.getName()); // FIXME

					result.add(new DefaultURIBaseImpl( // FIXME uri eleje kell? dropbox://dropbox.com/folder/file
							(metadata instanceof FolderMetadata) ? metadata.getName() + "/" : metadata.getName()));
					if (!res.getHasMore()) {
						break;
					}
					res = client.files().listFolderContinue(res.getCursor());
				}
				return result;
			}
		} catch (Throwable e) {
			log.warn("Operation failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl(uri.getPath());
		log.info("ATTRIBUTE: " + uri.getPath());
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			uriEntry.setLastModified(
					(Long) client.files().download(uri.getPath()).getResult().getClientModified().getTime()); // FIXME check it
			uriEntry.setSize(client.files().download(uri.getPath()).getResult().getSize());

		} catch (Throwable e) {
			log.warn("attributes failed!", e);
			throw new OperationException(e);
		}
		return uriEntry;
	}

	@Override
	public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession session,
			List<String> subentires) throws URIException, OperationException, CredentialException {
		if (subentires != null && subentires.size() > 0)
			throw new OperationException("Subentry filtering not supported");
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY)
			throw new URIException("URI is not a directory: " + uri.getURI());

		log.info("ATTRIBUTE: " + uri.getPath());
		List<URIBase> result = new Vector<URIBase>();
		/*
		 * try { DbxClientV2 client = getGoogleDriveClient(uri, credentials, session);
		 * 
		 * ListFolderResult res = client.files().listFolder("/"+uri.getPath());
		 * 
		 * while (true) { for (Metadata metadata : res.getEntries()) {
		 * log.info("google drive: " + metadata.getName()); DefaultURIBaseImpl uriEntry
		 * = new DefaultURIBaseImpl("/"+ metadata + "/");
		 * //uriEntry.setLastModified(client.files().download("/"+
		 * newFolder).getResult().getClientModified());
		 * uriEntry.setSize(client.files().download(metadata.getName()).getResult().
		 * getSize()); result.add(uriEntry); }
		 * 
		 * if (!res.getHasMore()) { break; } res =
		 * client.files().listFolderContinue(res.getCursor()); }
		 * 
		 * } catch (Exception x) { throw new OperationException(x); }
		 */
		return result;
	}

	@Override
	public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (!uri.getPath().endsWith("/"))
			throw new OperationException("URI must end with /!");

		// Upload file to Drop box
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			// Cut "/" from the file for example: /test/ => test
			log.info("testing make dir!: " + uri.getPath().substring(1, uri.getPath().length()-1));
			client.files().createFolderV2(uri.getPath().substring(1, uri.getPath().length()-1));
			// client.files().createFolderV2("/aaaabbbccc");
			log.info("FILE NAME TO CREATE!!!!: " + uri.getPath());
		} catch (Throwable e) {
			log.warn("make directory failed!", e);
			throw new OperationException(e);
		}

	}

	@Override
	public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (!uri.getPath().endsWith("/"))
			throw new OperationException("URI must end with /!");
//		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY)
//			throw new URIException("URI is not a directory: " + uri.getURI());

		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			// Cut "/" from the file for example: /test/ => test
			log.info(uri.getPath().substring(1, uri.getPath().length()-1));
			client.files().deleteV2(uri.getPath().substring(1, uri.getPath().length()-1));
		} catch (Throwable e) {
			log.warn("delete folder failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public void delete(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());

		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			client.files().deleteV2(uri.getPath());
		} catch (Throwable e) {
			log.warn("Delete file failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public void permissions(final URIBase uri, final Credentials credentials, final DataAvenueSession session,
			final String permissionsString) throws URIException, OperationException, CredentialException {
		throw new OperationException("Not implemented");
	}

	@Override
	public void rename(URIBase uri, String newName, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		throw new OperationException("Not implemented");
	}

	class InputStreamWrapper extends InputStream {
		InputStream is;
		DataAvenueSession session;

		InputStreamWrapper(InputStream is, DataAvenueSession session) {
			this.is = is;
			this.session = session;
		}

		@Override
		public int read() throws IOException {
			return is.read();
		}

		@Override
		public int read(byte b[]) throws IOException {
			return is.read(b);
		}

		@Override
		public int read(byte b[], int off, int len) throws IOException {
			return is.read(b, off, len);
		}

		@Override
		public void close() throws IOException {
			is.close();
		}
	}

	@Override
	public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		// open FileSystem, don't close
		InputStreamWrapper downloadedFile = null;
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			downloadedFile = new InputStreamWrapper(client.files().download(uri.getPath()).getInputStream(), session);
		} catch (Throwable e) {
			log.warn("Operation failed!", e);
			throw new OperationException(e);
		}
		return downloadedFile;
	}

	class OutputStreamWrapper extends OutputStream {
		OutputStream os;
		DataAvenueSession session;

		OutputStreamWrapper(OutputStream os, DataAvenueSession session) {
			this.os = os;
			this.session = session;
		}

		@Override
		public void write(int b) throws IOException {
			os.write(b);
		}

		@Override
		public void write(byte b[]) throws IOException {
			os.write(b);
		}

		@Override
		public void write(byte b[], int off, int len) throws IOException {
			os.write(b, off, len);
		}

		@Override
		public void flush() throws IOException {
			os.flush();
		}

		@Override
		public void close() throws IOException {
			os.close();
		}
	}

	@Override
	public OutputStream getOutputStream(final URIBase uri, final Credentials credentials,
			final DataAvenueSession dataAvenueSession, long contentLength)
			throws URIException, OperationException, CredentialException {

		throw new OperationException("Not implemented");
/* 
		log.info("UPLOAD FILE!!! " + uri.getPath());
		OutputStreamWrapper uploadedFile = null;

		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, dataAvenueSession);

			uploadedFile = new OutputStreamWrapper(client.files().upload("").getOutputStream(), dataAvenueSession);

		} catch (Throwable e) {
			log.warn("upload failed!", e);
			throw new OperationException(e);
		}
		return uploadedFile;
		*/
	}

	@Override
	public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session,
			InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException,
			IllegalArgumentException, OperationNotSupportedException {
		// use getOutputStream instead

		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			client.files().uploadBuilder(uri.getPath()).uploadAndFinish(inputStream);
		} catch (Throwable e) {
			log.warn("upload failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public String copy(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials,
			boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		throw new OperationException("Not implemented");
	}

	@Override
	public String move(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials,
			boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		throw new OperationException("Not implemented");
	}

	@Override
	public void cancel(String id) throws TaskIdException, OperationException {
		throw new OperationException("Not implemented");
	}

	@Override
	public long getFileSize(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		
		try {
			long fileLong = 0;
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			fileLong = client.files().download(uri.getPath()).getResult().getSize();
			return fileLong;
		} catch (Throwable e) {
			log.warn("file size failed!", e);
			throw new OperationException(e);
		}
	}

	public boolean exists(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			if (client.files().search("", uri.getPath()).getStart() > 0)
				return true;
			else 
				return false;
		} catch (Throwable e) {
			log.warn("Exist failed!", e);
			throw new OperationException(e);
		}
	}

	// test if object is readable
	@Override
	public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		return exists(uri, credentials, session);
	}

	@Override
	public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		return exists(uri, credentials, session);
	}

	@Override
	public void shutDown() {
		// no resources to free up
	}
}
