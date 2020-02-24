package hu.sztaki.lpds.dataavenue.adaptors.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationType;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationTypeList;
import hu.sztaki.lpds.dataavenue.interfaces.CloseableSessionObject;
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
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationTypeImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationTypeListImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * HDFS adaptor enables Data Avenue to connect to Apache Hadoop HDFS storages.
 * 
 * Warning: HDFS connections are not secured! (No Kerberos or other authentication/encryption of transferred data.)
 * Use Data Avenue within the same (safe) cluster where HDFS server resides behind firewall.
 * 
 * Note:
 * - replication number is: 1
 * 
 *  TODO:
 *  - check Kerberos authentication
 *  - re-check isReadable/isWritable permissions
 *  
 */
public class HDFSAdaptor implements Adaptor {
	
	private static final Logger log = LoggerFactory.getLogger(HDFSAdaptor.class);
	private static final String HDFS_PRPOTOCOL = "hdfs"; // scheme: hdfs://
	public static final String HDFS_CLIENTS = "hdfs_clients";
	public static final String NONE_AUTH = "None";
	private String version = "1.0.0";

	private static final Configuration hdfsConfiguration;
	
	static {
		hdfsConfiguration = new Configuration();
		try {
			// See: https://hadoop.apache.org/docs/r3.2.0/hadoop-project-dist/hadoop-common/core-default.xml
			hdfsConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			hdfsConfiguration.set("ipc.client.connect.timeout", "2000");
			hdfsConfiguration.set("ipc.client.connect.max.retries.on.timeouts", "2");
			hdfsConfiguration.set("dfs.client.use.datanode.hostname", "true");
			hdfsConfiguration.set("dfs.replication", "1");
			
//			conf.set("fs.defaultFS", hdfsuri); not needed
//			conf.set("ipc.client.connection.max.retries", "1"); does not seem to work
//			hdfsConfiguration.set("hadoop.http.staticuser.user", "dataavenue"); does not work to set username
			
			System.setProperty("HADOOP_USER_NAME", "dataavenue"); // set default username
		} catch (Throwable x) {
			log.error("Cannot configure HDFS", x);
		}
	}

	static class HDFSClient  implements CloseableSessionObject {
		private final Map<String, FileSystem> clients = new HashMap<String, FileSystem>(); // map: host -> client
		HDFSClient withClient(final URIBase uri) throws IOException {
			String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
			clients.put(hostAndPort, FileSystem.get(URI.create(uri.getURI()), hdfsConfiguration));
			return this;
		}
		FileSystem get(final URIBase uri) {
			String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
			return clients.get(hostAndPort);
		}
		@Override
		public void close() {
			for (FileSystem client: clients.values()) {
				try { client.close(); } 
				catch (Exception e) { log.warn("Cannot close HDFS client", e); }
			}			
		}
	}
	
	private FileSystem getHDFSClient(final URIBase uri, final Credentials credentials, final DataAvenueSession session) throws CredentialException, OperationException {
		HDFSClient clients = session != null ? (HDFSClient) session.get(HDFS_CLIENTS) : null;
		if (clients == null) {
			try {
				clients = new HDFSClient().withClient(uri);
			} catch (IOException x) { throw new OperationException(x); }
			if (session != null) session.put(HDFS_CLIENTS, clients);
		} 
		FileSystem client = clients.get(uri);
		if (client == null) throw new OperationException("APPLICATION ERROR: Cannot create HDFS client!"); 
		return client; 
	}
	
	
	@SuppressWarnings("unused")
	private void kerberosAuth() {
		/*
		// See: https://community.cloudera.com/t5/Community-Articles/A-Secure-HDFS-Client-Example/ta-p/247424
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","swebhdfs://one.hdp:50470");
		FileSystem fs = FileSystem.get(conf);
		conf.set("fs.defaultFS", "webhdfs://one.hdp:50070");
		conf.set("hadoop.security.authentication", "kerberos");

		java -Djava.security.auth.login.config=/home/hdfs-user/jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf -Djavax.security.auth.useSubjectCredsOnly=false
		com.sun.security.jgss.krb5.initiate {
    		com.sun.security.auth.module.Krb5LoginModule required
    		doNotPrompt=true
    		principal="hdfs-user@MYCORP.NET"
    		useKeyTab=true
    		keyTab="/home/hdfs-user/hdfs-user.keytab"
    		storeKey=true;
		};
		 */
	}
	
	public HDFSAdaptor() {
		String PROPERTIES_FILE_NAME = "META-INF/data-avenue-hdfs-adaptor.properties"; // try to read version number
		try {
			Properties prop = new Properties();
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME); // emits no exception but null returned
			if (in == null) { log.warn("Cannot find properties file: " + PROPERTIES_FILE_NAME); } 
			else {
				try {
					prop.load(in);
					for (Object key: prop.keySet()) System.out.println("Prop: " + key + " " + prop.getProperty((String) key));
					try { in.close(); } catch (IOException e) {}
					
					if (prop.get("version") != null) version = (String) prop.get("version");
					// no more properties to read
					
				} catch (Exception e) { log.warn("Cannot load properties from file: " + PROPERTIES_FILE_NAME); }
			}
		} catch (Throwable e) {log.warn("Cannot read properties file: " + PROPERTIES_FILE_NAME); } 
	}
	
   	/* adaptor meta information */
	@Override public String getName() { return "HDFS Adaptor"; }
	@Override public String getDescription() { return "HDFS Adaptor allows of connecting to Apache Hadoop HDFS storage"; }
	@Override public String getVersion() { return version; }
	@Override  
	public List<String> getSupportedProtocols() {
		List<String> result = new Vector<String>();
		result.add(HDFS_PRPOTOCOL);
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
		// no adaptor provided operations (such as copy between HDFS servers) 
		return Collections.<OperationsEnum>emptyList();
	}

	@Override 
	public List<String> getAuthenticationTypes(String protocol) {
		List<String> result = new Vector<String>();
		result.add(NONE_AUTH);
		// TODO authentication API? Kerberos?
		return result;
	}	
	
	@Override 
	public AuthenticationTypeList getAuthenticationTypeList(String protocol) {
		AuthenticationType a = new AuthenticationTypeImpl();
		a.setType(NONE_AUTH);
		a.setDisplayName("No authentication");
		AuthenticationTypeList l = new AuthenticationTypeListImpl();
		l.getAuthenticationTypes().add(a);
		return l;
	}	
	
	@Override 
	public String getAuthenticationTypeUsage(String protocol,	String authenticationType) {
		if (protocol == null || authenticationType == null) throw new IllegalArgumentException("null argument");
		if (NONE_AUTH.equals(authenticationType)) return "<b>" + NONE_AUTH + "</b> No authentication";
		return null;
	}
	
	// get file and directory names only in a directory
	@Override 
	public List<URIBase> list(final URIBase uri, Credentials credentials,	DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory: " + uri.getURI());
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			List<URIBase> result = new Vector<URIBase>();
	        FileStatus[] list = fs.listStatus(new Path(uri.getPath())); 
	        for (FileStatus entry: list) 
		        result.add(new DefaultURIBaseImpl(entry.getPath().toString() + (entry.isDirectory() ? URIBase.PATH_SEPARATOR : "")));
			return result;
		} catch (IOException x) {
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
	}

	// get attributes (file size, last mod time, permission) of a single file or directory
	@Override 
	public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
		 	FileStatus entry = fs.getFileStatus(new Path(uri.getPath()));
		 	DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl(entry.getPath().toString() + (entry.isDirectory() ? URIBase.PATH_SEPARATOR : ""));
	       	uriEntry.setLastModified(entry.getModificationTime());
	       	uriEntry.setSize(entry.getLen());
	       	uriEntry.setPermissions(entry.getPermission().toString());
	       	uriEntry.setDetails("Owner: " + entry.getOwner() + ", Group  " + entry.getGroup());
	       	return uriEntry;
		} catch (IOException x) {
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
	}
	
	// get attributes (file size, last mod time, permission) of all (or selected/filtered) files and subdirectries in a directory
	@Override 
	public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession session, List <String> subentires) throws URIException, OperationException, CredentialException {
		if (subentires != null && subentires.size() > 0) throw new OperationException("Subentry filtering not supported");
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory: " + uri.getURI());
		List<URIBase> result = new Vector<URIBase>();
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
	        FileStatus[] list = fs.listStatus(new Path(uri.getPath()));  // you need to pass in your hdfs path
	        for (FileStatus entry: list) {
	        	if (subentires != null && subentires.size() > 0)
	        		if (!subentires.contains(entry.getPath().getName() + (entry.isDirectory() ? URIBase.PATH_SEPARATOR : ""))) continue;
	        	DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl(entry.getPath().toString() + (entry.isDirectory() ? URIBase.PATH_SEPARATOR : ""));
	        	uriEntry.setLastModified(entry.getModificationTime());
	        	uriEntry.setSize(entry.getLen());
	        	uriEntry.setPermissions(entry.getPermission().toString());
	        	uriEntry.setDetails("Owner: " + entry.getOwner() + ", Group  " + entry.getGroup());
		        result.add(uriEntry);
	        }
		} catch (IOException x) {
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
		return result;
	}

	// create a directory (recursively: with parents if missing)
	@Override 
	public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory: " + uri.getURI());
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			fs.mkdirs(new Path(uri.getPath())); 
		} catch (IOException x) {
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
	}

	// delete directory recursively
	@Override 
	public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory: " + uri.getURI());
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			fs.delete(new Path(uri.getPath()), true); 
		} catch (IOException x) {
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
	}

	@Override 
	public void delete(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE && uri.getType() != URIBase.URIType.SYMBOLIC_LINK) throw new URIException("URI is not a file or symlink: " + uri.getURI());
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			fs.delete(new Path(uri.getPath()), false); 
		} catch (IOException x) {
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
	}
	
	// change permissions of a file or directory
	@Override 
	public void permissions(final URIBase uri, final Credentials credentials, final DataAvenueSession session, final String permissionsString) throws URIException, OperationException, CredentialException {
		if (permissionsString == null || permissionsString.length() != 9) throw new OperationException("Invalid permissions string");
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			fs.setPermission(new Path(uri.getPath()), new FsPermission(permissionsString)); 
		} catch (IOException x) {
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
	}
	
	// rename a file or a directory
	@Override 
	public void rename(URIBase uri, String newName, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE && uri.getType() != URIBase.URIType.DIRECTORY && uri.getType() != URIBase.URIType.SYMBOLIC_LINK) throw new URIException("URI is not a file or directory: " + uri.getURI());
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			String newPath = uri.getPath(); 
			if (uri.getType() == URIBase.URIType.DIRECTORY) {
				newPath = newPath.substring(0, newPath.length() - 1); // remove trailing /
				if (!newPath.contains(URIBase.PATH_SEPARATOR)) throw new OperationException("Cannot rename root path: " + uri.getPath());
				newPath = newPath.substring(0, newPath.lastIndexOf('/') + 1); // remove entry name
				newPath += newName + URIBase.PATH_SEPARATOR; // add new entry name, plus trailing /
			} else {
				newPath = newPath.substring(0, newPath.lastIndexOf('/') + 1); // remove old entry name
				newPath += newName; // add new name
			}
			fs.rename(new Path(uri.getPath()), new Path(newPath)); 
		} catch (IOException x) {
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
	}

	// return size of a file (use session if possible)
	@Override 
	public long getFileSize(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file: " + uri.getURI());
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			log.debug("Getting file size of uri.getPath()");
		 	FileStatus entry = fs.getFileStatus(new Path(uri.getPath()));
			log.debug("File size: " + entry.getLen());
		 	return entry.getLen();
		} catch (IOException x) {
			log.debug("getFileSize", x);
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
	}

	// return if a file is readable for the user
	@Override 
	public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file: " + uri.getURI());
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			if (!fs.exists(new Path(uri.getPath()))) return false;
		 	FileStatus entry = fs.getFileStatus(new Path(uri.getPath()));
		 	if (entry.getPermission().toString().contains("r")) return true; // FIXME check owner/group is me
		 	else return false;
		} catch (IOException x) {
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
	}

	// return if file is writable for the user
	@Override 
	public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file: " + uri.getURI());
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			if (!fs.exists(new Path(uri.getPath()))) return false;
		 	FileStatus entry = fs.getFileStatus(new Path(uri.getPath()));
		 	if (entry.getPermission().toString().contains("w")) return true; // FIXME check owner/group is me
		 	else return false;
		} catch (IOException x) {
			throw new OperationException(x);
		}
		finally {
			if (session == null && fs != null) try { fs.close(); } catch (Exception x) {} 
		}
	}
	
	class InputStreamWrapper extends InputStream {
		InputStream is;
		DataAvenueSession session;
		FileSystem client;
		InputStreamWrapper(InputStream is, DataAvenueSession session, FileSystem client) { this.is = is; this.session = session; this.client = client;}
		@Override public int read() throws IOException { return is.read(); }
		@Override public int read(byte b[]) throws IOException { return is.read(b); }
		@Override public int read(byte b[], int off, int len) throws IOException { return is.read(b, off, len); }
		@Override public void close() throws IOException {
			is.close();
			if (session == null && client != null) try { client.close(); } catch (Exception x) {} 
		}
	}
	
	// return file contents as stream
	@Override 
	public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file: " + uri.getURI());
		// open FileSystem, don't close
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			if (!fs.exists(new Path(uri.getPath()))) throw new OperationException("File does not exists: " + uri.getURI());
			return new InputStreamWrapper(fs.open(new Path(uri.getPath())), session, fs);
		} catch (IOException x) {
				throw new OperationException(x);
		}
	}

	class OutputStreamWrapper extends OutputStream {
		OutputStream os;
		DataAvenueSession session;
		FileSystem client;
		OutputStreamWrapper(OutputStream os,  DataAvenueSession session, FileSystem client) { this.os = os; this.session = session; this.client = client; }
		@Override public void write(int b) throws IOException {	os.write(b); }
		@Override  public void write(byte b[]) throws IOException { os.write(b); }
		@Override  public void write(byte b[], int off, int len) throws IOException { os.write(b, off, len); }
		@Override  public void flush() throws IOException { os.flush(); }
		@Override public void close() throws IOException {
			os.close();
			if (session == null && client != null) try { client.close(); } catch (Exception x) {} 
		}
	}
	
	@Override 
	public OutputStream getOutputStream(URIBase uri, Credentials credentials,	DataAvenueSession session, long size) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file: " + uri.getURI());
		FileSystem fs = getHDFSClient(uri, credentials, session);
		try {
			return new OutputStreamWrapper(fs.create(new Path(uri.getPath()), true), session, fs);
		} catch (IOException x) {
			throw new OperationException(x);
		}
	}
	
	@Override 
	public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session, InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException, OperationNotSupportedException {
		// use getOutputStream instead
		throw new OperationNotSupportedException("Not implemented");
    }

	@Override 
	public String copy(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		return copyOrMove(fromUri, fromCredentials, toUri, toCredentials, overwrite, monitor, false);
	}

	@Override 
	public String move(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		return copyOrMove(fromUri, fromCredentials, toUri, toCredentials, overwrite, monitor, true);
	}

	private String copyOrMove(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor, boolean isMove) throws URIException, OperationException, CredentialException {
		throw new OperationException("Not implemented");
	}
	
	@Override 
	public void cancel(String id) throws TaskIdException, OperationException {
		throw new OperationException("Not implemented");
	}
	
	@Override
	public void shutDown() {}
	
//	moved to WebContextListener
//	static {
//		try {
//			// needed to avoid conflict with UriBuilder abstract method jersey 1.19 vs. 2.17
//			javax.ws.rs.ext.RuntimeDelegate.setInstance(new org.glassfish.jersey.internal.RuntimeDelegateImpl());
//			log.info("Jersey 2.x RuntimeDelegate registered");
//		} catch (Throwable x) {
//			log.error("Cannot set JAX-RS 2.x org.glassfish.jersey.internal.RuntimeDelegateImpl (check jar: WEB-INF/lib/jersey-server-2.17 jar)", x);
//		}
//	}
}