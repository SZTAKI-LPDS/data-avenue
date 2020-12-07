package hu.sztaki.lpds.dataavenue.adaptors.dropbox;

import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.google.api.services.drive.Drive;

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

/**
 * @author Balint Rapolthy
 * 
 * Dropbox adaptor
 *
 */
public class DropboxAdaptor implements Adaptor {

	private static final Logger log = LoggerFactory.getLogger(DropboxAdaptor.class);

	private String adaptorVersion = "1.0.0"; // default adaptor version

	static final String PROTOCOL_PREFIX = "dropbox";

	static final String DROPBOX_CLIENT = "dropboxClient";
	static final String DROPBOX_HOST = "dropbox.com";

	static final String ACCESS_TOKEN = "accessToken";

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
		result.add(PROTOCOL_PREFIX);
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
		result.add(ACCESS_TOKEN);
		return result;
	}

	@Override
	public String getAuthenticationTypeUsage(String protocol, String authenticationType) {
		if (protocol == null || authenticationType == null)
			throw new IllegalArgumentException("null argument");
		if (ACCESS_TOKEN.equals(authenticationType))
			return "<b>" + ACCESS_TOKEN + "</b> (access token)";
		return null;
	}

	@Override
	public AuthenticationTypeList getAuthenticationTypeList(String arg0) {

		AuthenticationTypeList l = new AuthenticationTypeListImpl();

		AuthenticationType a = new AuthenticationTypeImpl();

		a = new AuthenticationTypeImpl();
		a.setType(ACCESS_TOKEN);
		a.setDisplayName("Dropbox credential");

		AuthenticationField f = new AuthenticationFieldImpl();
		f.setKeyName(ACCESS_TOKEN);
		f.setDisplayName("Dropbox access token");
		a.getFields().add(f);

		l.getAuthenticationTypes().add(a);

		return l;
	}

	private DbxClientV2 getDropboxClient(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {

		DbxClientV2 client = (session != null) ? (DbxClientV2) session.get(DROPBOX_CLIENT) : null;

		// try to get client from session...
		if (client != null)
			return client;

		// not found in session...

		// check uri
		if (!DROPBOX_HOST.equals(uri.getHost()))
			throw new URIException("Hostname must be: " + DROPBOX_HOST);

		// check credentials
		if (credentials == null)
			throw new CredentialException("No credential!");

		String accessKey = credentials.getCredentialAttribute(ACCESS_TOKEN);

		if (accessKey == null) // check credentials
			throw new CredentialException("Missing " + ACCESS_TOKEN + " credential!");

		DbxRequestConfig config = new DbxRequestConfig("data avenue");
		client = new DbxClientV2(config, accessKey);

		// add newly created client to session, if there is session
		if (session != null)
			session.put(DROPBOX_CLIENT, client);

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

			// Name of the client user
			log.info("dropbox: " + client.users().getCurrentAccount().getName());

			// Get files and folder metadata from Drop box directory
			log.info("list path: " + uri.getPath());

			// Get all the entries in the given directory
			ListFolderResult userFiles = client.files().listFolder(URIBase.PATH_SEPARATOR + uri.getPath());
			for (Metadata metadata : userFiles.getEntries()) {
				// Check the type of the metadata
				result.add(new DefaultURIBaseImpl((metadata instanceof FolderMetadata)
						? uri.getURI() + metadata.getName() + URIBase.PATH_SEPARATOR
						: uri.getURI() + metadata.getName()));
			}
			return result;
		} catch (Throwable e) {
			log.warn("list failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl(uri.getPath());
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			
			// Get files' proerties
			uriEntry.setLastModified(client.files().getTemporaryLink(uri.getPath()).getMetadata().getClientModified().getTime());
			uriEntry.setSize(client.files().download(uri.getPath()).getResult().getSize());
		} catch (Throwable e) {
			log.warn("attribute file failed!", e);
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

		List<URIBase> result = new Vector<URIBase>();

		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			ListFolderResult res = client.files().listFolder(URIBase.PATH_SEPARATOR + uri.getPath());

			while (true) {
				for (Metadata metadata : res.getEntries()) {
					if (subentires != null && subentires.size() > 0)
						if (!subentires.contains((metadata instanceof FolderMetadata)
								? metadata.getPathDisplay() + URIBase.PATH_SEPARATOR
								: metadata.getPathDisplay()))
							continue;
					if (metadata instanceof FolderMetadata) {
						// If the entry is directory, then set size null
						DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl(metadata.getPathDisplay() + URIBase.PATH_SEPARATOR);
						uriEntry.setSize(null);
						result.add(uriEntry);
					} else {
						
						// Files' properties queried
						DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl(metadata.getPathLower());
						uriEntry.setLastModified(((FileMetadata) metadata).getClientModified().getTime());
						uriEntry.setSize(((FileMetadata) metadata).getSize());
						result.add(uriEntry);
					}
				}
				if (!res.getHasMore()) {
					break;
				}
				res = client.files().listFolderContinue(res.getCursor());
			}
		} catch (Throwable e) {
			log.warn("attribute list failed!", e);
			throw new OperationException(e);
		}
		return result;
	}

	@Override
	public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (!uri.getPath().endsWith(URIBase.PATH_SEPARATOR))
			throw new OperationException("URI must end with /!");
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY)
			throw new URIException("URI is not a directory: " + uri.getURI());
		// Create folder to Drop box
		try {
			// Create a Pattern object
			Pattern r = Pattern.compile("[a-zA-Z]");
			// Now create matcher object.
			Matcher m = r.matcher(uri.getEntryName());
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			// Cut "/" from the file for example: /test/ => test
			log.info("testing make dir!: " + uri.getEntryName());
			if (!m.find()) {
				throw new OperationException("Only A-Z characters are allowed!");
			}
			if (uri.getEntryName().length() > 20) {
				throw new OperationException("Filename can not be longer than 20 character!");
			}
			client.files().createFolderV2(uri.getPath().substring(0, uri.getPath().length()-1));
		} catch (Throwable e) {
			log.warn("make directory failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (!uri.getPath().endsWith(URIBase.PATH_SEPARATOR))
			throw new OperationException("URI must end with /!");
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY)
			throw new URIException("URI is not a directory: " + uri.getURI());
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			// Delete directory
			log.info("delete folder: " +uri.getPath().substring(0,uri.getPath().length()-1));
			client.files().deleteV2(uri.getPath().substring(0,uri.getPath().length()-1));
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
			// Delete a file
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
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			// Move function is used to rename the file, for example: /folder/abc.txt => /folder/cba.txt
			String newPath = uri.getPath();
			// Create a Pattern object
			Pattern r = Pattern.compile("[a-zA-Z]");
			//  matcher object.
			Matcher m = r.matcher(newName);
			if (!m.find()) {
				throw new OperationException("Only A-Z characters are allowed!");
			}
			if (newName.length() > 20) {
				throw new OperationException("Filename can not be longer than 20 character!");
			}
			newPath = newPath.substring(0, newPath.lastIndexOf(URIBase.PATH_SEPARATOR) + 1);
			newPath += newName;
			log.info("Path of the file " + newPath + " " + uri.getPath());
			// Change the file path with the name
			client.files().moveV2(uri.getPath(), newPath);
		} catch (Throwable e) {
			log.warn("Rename file failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			// Download a file with the getInputSteam 
			return client.files().download(uri.getPath()).getInputStream();
		} catch (Throwable e) {
			log.warn("Download failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public OutputStream getOutputStream(final URIBase uri, final Credentials credentials,
			final DataAvenueSession dataAvenueSession, long contentLength)
			throws URIException, OperationException, CredentialException {
		log.info("UPLOAD FILE " + uri.getPath());
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, dataAvenueSession);
			return client.files().upload(uri.getPath()).getOutputStream();
		} catch (Throwable e) {
			log.warn("upload failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session,
			InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException,
			IllegalArgumentException, OperationNotSupportedException {
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			// Upload a file with the InputStream
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
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			// getTemporaryLink can recover
			return client.files().getTemporaryLink(uri.getPath()).getMetadata().getSize();
		} catch (Throwable e) {
			log.warn("file size failed!", e);
			throw new OperationException(e);
		}
	}
	
	// This is used in the isReadable and isWritable functions
	public boolean exists(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		try {
			DbxClientV2 client = getDropboxClient(uri, credentials, session);
			// Check for the file in the substring of the directory
			long count = uri.getPath().chars().filter(ch -> ch == '/').count();
			String path = (count > 1)? uri.getPath().substring(0, uri.getPath().lastIndexOf(URIBase.PATH_SEPARATOR)) : "";
			log.info("path: "+ path);
			
			ListFolderResult res = client.files().listFolder(path);
			for (Metadata metadata : res.getEntries()) {
				if(metadata.getName().equals(uri.getEntryName())) return true;
			}
			return false;
		} catch (Throwable e) {
			log.warn("Exists failed!", e);
			throw new OperationException(e);
		}
	}

	// test if object is readable
	@Override
	public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		return exists(uri, credentials, session);
	}

	@Override
	public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		return exists(uri, credentials, session);
	}

	@Override
	public void shutDown() {
		// no resources to free up
	}
}
