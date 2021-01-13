package hu.sztaki.lpds.dataavenue.adaptors.googledrive;

import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.Permission;

import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.AsyncCommands;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationField;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationType;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationTypeList;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum;
import hu.sztaki.lpds.dataavenue.interfaces.SyncCommands;
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
 * Google Drive adaptor
 *
 */
public class GoogleDriveAdaptor implements Adaptor {

	private static final Logger log = LoggerFactory.getLogger(GoogleDriveAdaptor.class);

	private String adaptorVersion = "1.0.0"; // default adaptor version

	static final String PROTOCOL_PREFIX = "googledrive";
	static final String PROTOCOLS = "googledrive";
	static final String ACCESS_KEY_CREDENTIAL = "accessKey";
	static final String GOOGLEDRIVE_ClIENT = "googledriveClient";
	static final String GOOGLEDRIVE_HOST = "googledrive.com";

	public GoogleDriveAdaptor() {
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
		return "Google Drive Adaptor";
	}

	@Override
	public String getDescription() {
		return "Google Drive Adaptor allows of connecting to Google drive storages";
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
			return "<b>" + ACCESS_KEY_CREDENTIAL + "</b> (access key)";
		return null;
	}

	@Override
	public AuthenticationTypeList getAuthenticationTypeList(String arg0) {

		AuthenticationTypeList l = new AuthenticationTypeListImpl();

		AuthenticationType a = new AuthenticationTypeImpl();

		a = new AuthenticationTypeImpl();
		a.setType(ACCESS_KEY_CREDENTIAL);
		a.setDisplayName("Google Drive credential");

		AuthenticationField f1 = new AuthenticationFieldImpl();
		f1.setKeyName(ACCESS_KEY_CREDENTIAL);
		f1.setDisplayName("Google Drive access key");
		a.getFields().add(f1);

		l.getAuthenticationTypes().add(a);

		return l;
	}

	private Drive getGoogleDriveClient(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws OperationException, GeneralSecurityException, CredentialException, IOException, URIException {
		Drive client = (session != null) ? (Drive) session.get(GOOGLEDRIVE_ClIENT) : null;

		if (client != null)
			return client;

		// check credentials
		if (credentials == null)
			throw new CredentialException("No credentials!");

		// check uri
		if (!GOOGLEDRIVE_HOST.equals(uri.getHost()))
			throw new URIException("Hostname must be: " + GOOGLEDRIVE_HOST);

		// check credentials with API
		String accessKey = credentials.getCredentialAttribute(ACCESS_KEY_CREDENTIAL);
		GoogleCredential credential = new GoogleCredential().setAccessToken(accessKey);
		client = new Drive.Builder(GoogleNetHttpTransport.newTrustedTransport(), JacksonFactory.getDefaultInstance(),
				credential).setApplicationName("data-avenue").build();
		if (session != null)
			session.put(GOOGLEDRIVE_ClIENT, client);
		return client;
	}

	// Here it is possible to query any file or folder
	private File getFileByName(String fileName, Drive client, Boolean isFolder) throws IOException {
		return client.files().list()
				.setQ("name = '" + fileName + "' and mimeType " + ((isFolder) ? "=" : "!=")
						+ " 'application/vnd.google-apps.folder'")
				.setFields("files(id, name, size, modifiedTime)").execute().getFiles().get(0);
	}

	@Override
	public List<URIBase> list(final URIBase uri, Credentials credentials, DataAvenueSession session)
			throws OperationException, URIException {
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY)
			throw new URIException("URI is not a URL or a directory!");
		try {
			List<URIBase> result = new Vector<URIBase>();
			Drive client = getGoogleDriveClient(uri, credentials, session);
			File getFolder = (uri.getEntryName().length() > 1) ? getFileByName(uri.getEntryName(), client, true) : null;

			// Query folders
			FileList driveFiles = client.files().list()
					.setQ((getFolder == null) ? "mimeType ='application/vnd.google-apps.folder'"
							: "'" + getFolder.getId()
									+ "' in parents and mimeType ='application/vnd.google-apps.folder'")
					.setPageSize(20)
					.execute();

			List<File> files = driveFiles.getFiles();

			if (files == null || files.isEmpty()) {
			} else {
				for (File file : files) {
					result.add(new DefaultURIBaseImpl(URIBase.PATH_SEPARATOR + file.getName() + URIBase.PATH_SEPARATOR));
				}
			}
			
			driveFiles = client.files().list()
					.setQ((getFolder == null) ? "mimeType !='application/vnd.google-apps.folder'"
							: "'" + getFolder.getId()
									+ "' in parents and mimeType !='application/vnd.google-apps.folder'")
					.execute();

			files = driveFiles.getFiles();
			if (files == null || files.isEmpty()) {
			} else {
				for (File file : files) {
					result.add(new DefaultURIBaseImpl(URIBase.PATH_SEPARATOR + file.getName()));
				}
			}
			return result;
		} catch (Throwable e) {
			log.warn("listing entries failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl(URIBase.PATH_SEPARATOR + uri.getEntryName());
		log.info("ATTRIBUTE: " + uri.getEntryName());
		try {
			Drive client = getGoogleDriveClient(uri, credentials, session);
			File getFile = getFileByName(uri.getEntryName(), client, false);
			uriEntry.setLastModified(getFile.getModifiedTime().getValue());
			uriEntry.setSize(getFile.getSize());
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
			Drive client = getGoogleDriveClient(uri, credentials, session);
			File getFolder = (uri.getEntryName().length() > 1) ? getFileByName(uri.getEntryName(), client, true) : null;

			// Query folders
			FileList driveFiles = client.files().list()
					.setQ((getFolder == null) ? "mimeType ='application/vnd.google-apps.folder'"
							: "'" + getFolder.getId()
									+ "' in parents and mimeType ='application/vnd.google-apps.folder'")
					.setPageSize(20)
					.execute();

			List<File> files = driveFiles.getFiles();

			if (files == null || files.isEmpty()) {
			} else {
				for (File file : files) {
					if (subentires != null && subentires.size() > 0)
						if (!subentires.contains(URIBase.PATH_SEPARATOR + file.getName() + URIBase.PATH_SEPARATOR))
							continue;

					DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl(
							URIBase.PATH_SEPARATOR + file.getName() + URIBase.PATH_SEPARATOR);
					uriEntry.setSize(null);
					result.add(uriEntry);
				}
			}

			driveFiles = client.files().list()
					.setQ((getFolder == null) ? "mimeType !='application/vnd.google-apps.folder'"
							: "'" + getFolder.getId()
									+ "' in parents and mimeType !='application/vnd.google-apps.folder'")
					.setFields("files(id, name, size, modifiedTime)")
					.execute();

			files = driveFiles.getFiles();

			if (files == null || files.isEmpty()) {
			} else {
				for (File file : files) {
					if (subentires != null && subentires.size() > 0)
						if (!subentires.contains(URIBase.PATH_SEPARATOR + file.getName()))
							continue;

					DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl(URIBase.PATH_SEPARATOR + file.getName());
					uriEntry.setSize(file.getSize());
					uriEntry.setLastModified(file.getModifiedTime().getValue());
					result.add(uriEntry);
				}
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
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY)
			throw new URIException("URI is not a URL or a directory!");
		if (!uri.getPath().endsWith(URIBase.PATH_SEPARATOR))
			throw new OperationException("URI must end with /!");
		try {
			Drive client = getGoogleDriveClient(uri, credentials, session);
			log.info("new path = " + uri.getPath());
			
			long count = uri.getPath().chars().filter(ch -> ch == '/').count();
			String path = "";
			if(count >= 3) {
				String path1 = uri.getPath().substring(0, uri.getPath().length()-1);
				path = path1.substring(0, path1.lastIndexOf(URIBase.PATH_SEPARATOR)+1);
				if(count > 3) {
					path = path.substring(0, path.length()-1);
					path = path.substring(path.lastIndexOf(URIBase.PATH_SEPARATOR)+1);
				} else {
					path = path.substring(1, path.length()-1);
				}
			}
			log.info("new path = " + path + " " + uri.getEntryName());
			// Creating and checking new name with Regex
			Pattern r = Pattern.compile("[a-zA-Z]");
			Matcher m = r.matcher(uri.getEntryName());
			if (!m.find()) {
				throw new OperationException("Only A-Z characters are allowed!");
			}
			if (uri.getEntryName().length() > 20) {
				throw new OperationException("Filename can not be longer than 20 character!");
			}
			File fileMetadata = new File();
			fileMetadata.setName(uri.getEntryName());
			fileMetadata.setMimeType("application/vnd.google-apps.folder");
			if(path != "") {
				File file = getFileByName(path, client, true);
				fileMetadata.setParents(Collections.singletonList(file.getId()));
			}
			client.files().create(fileMetadata).execute();
		} catch (Throwable e) {
			log.warn("Operation failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY)
			throw new URIException("URI is not a URL or a directory!");
		if (!uri.getPath().endsWith(URIBase.PATH_SEPARATOR))
			throw new OperationException("URI must end with /!");
		try {
			Drive client = getGoogleDriveClient(uri, credentials, session);
			log.info("ID of the file: " + uri.getEntryName());
			File file = getFileByName(uri.getEntryName(), client, true);
			client.files().delete(file.getId()).execute();
		} catch (Throwable e) {
			log.warn("Operation failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public void delete(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		try {
			Drive client = getGoogleDriveClient(uri, credentials, session);
			File file = getFileByName(uri.getEntryName(), client, false);
			log.info("ID of the file: " + uri.getPath());
			client.files().delete(file.getId()).execute();
		} catch (Throwable e) {
			log.warn("delete failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public void permissions(final URIBase uri, final Credentials credentials, final DataAvenueSession session,
			final String permissionsString) throws URIException, OperationException, CredentialException {
		if (uri.getPath().endsWith(URIBase.PATH_SEPARATOR))
			throw new OperationException("URI must end with /!");
		try {
			Drive client = getGoogleDriveClient(uri, credentials, session);
			File file = getFileByName(uri.getEntryName(), client, false);
			Permission content = new Permission();
			content.setRole(permissionsString);
			client.permissions().create(file.getId(), content);
		} catch (Throwable e) {
			log.warn("permission failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public void rename(URIBase uri, String newName, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		try {
			Drive client = getGoogleDriveClient(uri, credentials, session);
			File file = getFileByName(uri.getEntryName(), client, false);
			// Create a Pattern object
			Pattern r = Pattern.compile("[a-zA-Z]");
			// Now create matcher object.
			Matcher m = r.matcher(newName);
			if (!m.find()) {
				throw new OperationException("Only A-Z characters are allowed!");
			}
			if (newName.length() > 20) {
				throw new OperationException("Filename can not be longer than 20 character!");
			}
			file.setName(newName);
			java.io.File fileContent = new java.io.File(newName);
			FileContent mediaContent = new FileContent(file.getFileExtension(), fileContent);
			client.files().update(file.getId(), file, mediaContent).execute();
		} catch (Throwable e) {
			log.warn("rename failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		try {
			Drive client = getGoogleDriveClient(uri, credentials, session);
			// Download file from the cloud storage
			File file = getFileByName(uri.getEntryName(), client, false);
			return client.files().get(file.getId()).executeMediaAsInputStream();
		} catch (Throwable e) {
			log.warn("download failed!", e);
			throw new OperationException(e);
		}
	}

	@Override
	public OutputStream getOutputStream(final URIBase uri, final Credentials credentials,
			final DataAvenueSession dataAvenueSession, long contentLength)
			throws URIException, OperationException, CredentialException {
		throw new OperationException("Not implemented");
	}

	@Override
	public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session,
			InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException,
			IllegalArgumentException, OperationNotSupportedException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		log.info("name of the file:" + uri.getEntryName());

		try {
			Drive client = getGoogleDriveClient(uri, credentials, session);
			long count = uri.getPath().chars().filter(ch -> ch == '/').count();
			String path = "";
			if(count >= 2) {
				String path1 = uri.getPath().substring(0, uri.getPath().length()-1);
				path = path1.substring(0, path1.lastIndexOf(URIBase.PATH_SEPARATOR)+1);
				if(count > 2) {
					path = path.substring(0, path.length()-1);
					path = path.substring(path.lastIndexOf(URIBase.PATH_SEPARATOR)+1);
				} else {
					path = path.substring(1, path.length()-1);
				}
			}
			File file = new File();
			file.setName(uri.getEntryName());
			if(path != "") {
				File getFolder = getFileByName(path, client, true);
				file.setParents(Collections.singletonList(getFolder.getId()));
			}
			// Upload a specific file to storage
			client.files().create(file, new InputStreamContent("binary/octet-stream",
					new ByteArrayInputStream(IOUtils.toByteArray(inputStream)))).execute();
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
			Drive client = getGoogleDriveClient(uri, credentials, session);
			// Get size of file
			return getFileByName(uri.getEntryName(), client, false).getSize();
		} catch (Throwable e) {
			log.warn("size failed!", e);
			throw new OperationException(e);
		}
	}

	// Check the file's existence in the user's storage
	public boolean exists(URIBase uri, Credentials credentials, DataAvenueSession session)
			throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE)
			throw new URIException("URI is not a file: " + uri.getURI());
		try {
			Drive client = getGoogleDriveClient(uri, credentials, session);
			long count = uri.getPath().chars().filter(ch -> ch == '/').count();
			String path = "";
			if(count >= 2) {
				String path1 = uri.getPath().substring(0, uri.getPath().length()-1);
				path = path1.substring(0, path1.lastIndexOf(URIBase.PATH_SEPARATOR)+1);
				if(count > 2) {
					path = path.substring(0, path.length()-1);
					path = path.substring(path.lastIndexOf(URIBase.PATH_SEPARATOR)+1);
				} else {
					path = path.substring(1, path.length()-1);
				}
			}
			File getFolder = (path.length() > 1) ? getFileByName(path, client, true) : null;

			// Query folders
			log.info("folder:  " + path);
			
			return client.files().list()
					.setQ((getFolder == null) ? "mimeType !='application/vnd.google-apps.folder' and name = '" +  uri.getEntryName()  + "'"
							: "'" + getFolder.getId()
									+ "' in parents and name = '" +  uri.getEntryName() + "'")
					.execute().getFiles().size() > 0;
		} catch (Throwable e) {
			log.warn("exists failed!", e);
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
