package hu.sztaki.lpds.dataavenue.adaptors.s3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.EmailAddressGrantee;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.MultiObjectDeleteException.DeleteError;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationField;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationType;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationTypeList;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.DirectURLsSupported;
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
import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.*;

/* Amazon defaults:
 		public static String S3_HOSTNAME = "s3.amazonaws.com";
		public static String S3_SERVICE_NAME = "Amazon S3";
 */
public class S3Adaptor implements Adaptor, DirectURLsSupported {
	private static final Logger log = LoggerFactory.getLogger(S3Adaptor.class);
	private static final String S3_PRPOTOCOL = "s3"; // scheme: s3://
	public static final String S3_CLIENTS = "s3_clients";
	private String version = "3.0.0"; // default
	
	static final String ACCESS_KEY_CREDENTIAL = "accessKey";
	static final String LEGACY_ACCESS_KEY_CREDENTIAL = "UserID";
	static final String SECRET_KEY_CREDENTIAL = "secretKey";
	static final String LEGACY_SECRET_KEY_CREDENTIAL = "UserPass";
	
	public S3Adaptor() {
		String PROPERTIES_FILE_NAME = "META-INF/data-avenue-s3-adaptor.properties"; // try to read version number
		try {
			Properties prop = new Properties();
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME); // emits no exception but null returned
			if (in == null) { log.warn("Cannot find properties file: " + PROPERTIES_FILE_NAME); } 
			else {
				try {
					prop.load(in);
					for (Object key: prop.keySet()) {
						System.out.println("Prop: " + key + " " + prop.getProperty((String) key));
					}
				
					try { in.close(); } catch (IOException e) {}
					if (prop.get("version") != null) version = (String) prop.get("version");
					if (prop.get("connections") != null) {
						int value = Integer.parseInt((String) prop.get("connections"));
						if (value > 0) S3Clients.MAX_CONNECTIONS = value; // else leave default 100
					}
					
					log.info("HTTPS is used on all ports except for port 80 (HTTP)...");
					if (prop.get("disableHostnameVerification") != null && !"no".equals(prop.get("disableHostnameVerification"))) {
						S3Clients.DISABLE_HOSTNAME_VERIFICATION = true;
						log.warn("Hostname verification disabled");
					} else {
						log.info("Hostname verification enabled");
					}
					log.debug("S3 Adaptor version: " + version + " (max. client connections = " + S3Clients.MAX_CONNECTIONS + ")" );
					
					if (prop.get("partSize") != null) { //
						try {
							int value = Integer.parseInt((String) prop.get("partSize"));
							if (value >= 5 * 1024 * 1024) ThreadedMultipartUploadOutputStream.DEFAULT_PART_SIZE = value; // else leave default
						} catch (NumberFormatException x) { log.error("Invalid integer in properties file: " + prop.get("partSize")); }
					}
					if (prop.get("multipartUploadThreads") != null) { //
						try {
							int value = Integer.parseInt((String) prop.get("multipartUploadThreads"));
							if (value >= 1 && value <= 100) ThreadedMultipartUploadOutputStream.DEFAULT_MAX_UPLOAD_THREADS = value; // else leave default
						} catch (NumberFormatException x) { log.error("Invalid integer in properties file: " + prop.get("partSize")); }
					}
					if (prop.get("putObjectLimit") != null) { //
						try {
							long value = Long.parseLong((String) prop.get("putObjectLimit"));
							if (value >= 0) PutObjectOutputStream.PUT_OBJECT_LIMIT = value; // else leave default
						} catch (NumberFormatException x) { log.error("Invalid long in properties file: " + prop.get("putObjectLimit")); }
					}
					
					log.info("ThreadedMultipartUploadOutputStream.PART_SIZE: " + ThreadedMultipartUploadOutputStream.DEFAULT_PART_SIZE);
					log.info("ThreadedMultipartUploadOutputStream.DEFAULT_MAX_UPLOAD_THREADS: " + ThreadedMultipartUploadOutputStream.DEFAULT_MAX_UPLOAD_THREADS);
					log.info("PutObjectOutputStream.PUT_OBJECT_LIMIT: " + PutObjectOutputStream.PUT_OBJECT_LIMIT);
										
				} catch (Exception e) { log.warn("Cannot load properties from file: " + PROPERTIES_FILE_NAME); }
			}
		} catch (Throwable e) {log.warn("Cannot read properties file: " + PROPERTIES_FILE_NAME); } 
	}
   
	static AmazonS3Client getAmazonS3Client(final URIBase uri, final Credentials credentials, final DataAvenueSession session) throws CredentialException {
		S3Clients clients = session != null ? (S3Clients) session.get(S3_CLIENTS) : null;

		if (clients == null) {
			if (credentials == null) throw new CredentialException("No credentials!");

			if (credentials.getCredentialAttribute(ACCESS_KEY_CREDENTIAL) == null) credentials.putCredentialAttribute(ACCESS_KEY_CREDENTIAL, credentials.getCredentialAttribute(LEGACY_ACCESS_KEY_CREDENTIAL)); // legacy
			if (credentials.getCredentialAttribute(SECRET_KEY_CREDENTIAL) == null) credentials.putCredentialAttribute(SECRET_KEY_CREDENTIAL, credentials.getCredentialAttribute(LEGACY_SECRET_KEY_CREDENTIAL)); // legacy
			String accessKey = credentials.getCredentialAttribute(ACCESS_KEY_CREDENTIAL); 
			String secretKey = credentials.getCredentialAttribute(SECRET_KEY_CREDENTIAL);
			
			clients = new S3Clients().withClient(uri, accessKey, secretKey);
			if (session != null) session.put(S3_CLIENTS, clients);
		} 
		
		AmazonS3Client client = clients.get(uri);
		if (client == null) throw new AmazonClientException("APPLICATION ERROR: Cannot create Amazon S3 client!"); 
		
		return client; 
	}
	
	/* adaptor meta information */
	@Override public String getName() { return "S3 Adaptor"; }
	@Override public String getDescription() { return "S3 Adaptor allows of connecting to cloud storages via Amazon S3 interface"; }
	@Override public String getVersion() { return version; }
	@Override  public List<String> getSupportedProtocols() {
		List<String> result = new Vector<String>();
		result.add(S3_PRPOTOCOL);
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
		result.add("UserPass");
		return result;
	}	
	
	@Override public AuthenticationTypeList getAuthenticationTypeList(String protocol) {
		AuthenticationType a = new AuthenticationTypeImpl();
		a.setType("UserPass");
		a.setDisplayName("S3 authentication");
		
		AuthenticationField f1 = new AuthenticationFieldImpl();
		f1.setKeyName(ACCESS_KEY_CREDENTIAL); // "UserID"
		f1.setDisplayName("Access key");
		a.getFields().add(f1);
		
		AuthenticationField f2 = new AuthenticationFieldImpl();
		f2.setKeyName(SECRET_KEY_CREDENTIAL); // "UserPass"
		f2.setDisplayName("Secret key");
		f2.setType(AuthenticationField.PASSWORD_TYPE);
		a.getFields().add(f2);
		
		AuthenticationTypeList l = new AuthenticationTypeListImpl();
		l.getAuthenticationTypes().add(a);
		
		return l;
	}	
	
	@Override public String getAuthenticationTypeUsage(String protocol,
			String authenticationType) {
		if (protocol == null || authenticationType == null) throw new IllegalArgumentException("null argument");
		if ("UserPass".equals(authenticationType)) return "<b>accessKey</b> (access key), <b>secretKey</b> (secret key)";
		return null;
	}
	
	@Override public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		S3URIImpl s3Uri = new S3URIImpl(uri.getURI());
		// note: cannot query attributes of / (root) 
		if (s3Uri.getType() != URIType.FILE && s3Uri.getType() != URIType.DIRECTORY) throw new URIException("Cannot query attributes of root /" + s3Uri.getURI());
		// URI is a bucket | dir | file name
		AmazonS3Client client = getAmazonS3Client(s3Uri, credentials, session);
		try {
			if (s3Uri.getPathWithinBucket() == null) { // bucket
				for (Bucket bucket: client.listBuckets()) { // uri contains no bucket name
					if (bucket.getName().equals(s3Uri.getBucketName())) {
						if (bucket.getCreationDate() != null) s3Uri.setLastModified(bucket.getCreationDate().getTime());
						try { s3Uri.setPermissions(getPermissions(client.getBucketAcl(bucket.getName()))); } catch (Exception e) {} // not permitted to query bucket ACL
						s3Uri.setDetails(bucket.getOwner() != null && !"".equals(bucket.getOwner().getDisplayName())? "Owner: " + bucket.getOwner().getDisplayName() + " (" + bucket.getOwner().getId() + ")" : null);
						s3Uri.setSize(0l); // buckets has 0 size
						return s3Uri;
					}
				}
				throw new OperationException("Bucket name not found: " + s3Uri.getBucketName());
				
			} else { // object (dir/file)
				log.debug("get metadata: " + s3Uri.getBucketName() + " " + s3Uri.getPathWithinBucket().substring(1));
				
				ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName(s3Uri.getBucketName()).withDelimiter(S3URIImpl.DELIMITER).withPrefix(s3Uri.getPathWithinBucket().substring(1));
				ObjectListing objectListing = client.listObjects(listObjectRequest); // returns 1 long list
				if (objectListing != null) for (S3ObjectSummary objectSummary: objectListing.getObjectSummaries()) {
					s3Uri.setSize(objectSummary.getSize());
					if (objectSummary.getLastModified() != null) s3Uri.setLastModified(objectSummary.getLastModified().getTime());
				}		
				/*
				NOTE: does not work on Amazon 
				ObjectMetadata objectSummary = client.getObjectMetadata(s3Uri.getBucketName(), s3Uri.getPathWithinBucket().substring(1)); // FIXME
			    if (objectSummary.getContentLength() >= 0) s3Uri.setSize(objectSummary.getContentLength());
			    if (objectSummary.getLastModified() != null) s3Uri.setLastModified(objectSummary.getLastModified().getTime());
			    */
			    AccessControlList acl = client.getObjectAcl(s3Uri.getBucketName(), s3Uri.getPathWithinBucket().substring(1));
			    try { s3Uri.setPermissions(getPermissions(acl)); } catch (Exception e) {} // not permitted to query bucket ACL
			    if (acl.getOwner() != null && !"".equals(acl.getOwner().getDisplayName())) s3Uri.setDetails("Owner: " + acl.getOwner().getDisplayName() + " (" + acl.getOwner().getId() + ")");
			    return s3Uri;
			}
		}
		catch (AmazonServiceException e) {
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: " + e.getErrorCode() + "", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); throw new OperationException("Operation exception: '" + e.getMessage() + "'!", e); }
		finally {
			if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} 
		}
	}
	
	@Override public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession session, List <String> subentires) throws URIException, OperationException, CredentialException {
		List<URIBase> result = new Vector<URIBase>();
		boolean truncated = false;
		S3URIImpl s3Uri = new S3URIImpl(uri.getURI());
		AmazonS3Client client = getAmazonS3Client(s3Uri, credentials, session);
		try {
			if (s3Uri.getBucketName() == null) { // list bucket names
				for (Bucket bucket: client.listBuckets()) { // uri contains no bucket name
					if (subentires != null && subentires.size() > 0 && !subentires.contains(bucket.getName() + S3URIImpl.DELIMITER)) continue; // filter subentries
					S3URIImpl bucketURI = new S3URIImpl(s3Uri.getURI() + bucket.getName() + (!bucket.getName().endsWith(S3URIImpl.DELIMITER) ? S3URIImpl.DELIMITER : "") ); 
					if (bucket.getCreationDate() != null) bucketURI.setLastModified(bucket.getCreationDate().getTime());
					try { bucketURI.setPermissions(getPermissions(client.getBucketAcl(bucket.getName()))); } catch (Exception e) {} // not permitted to query bucket ACL
					bucketURI.setDetails(bucket.getOwner() != null && !"".equals(bucket.getOwner().getDisplayName())? "Owner: " + bucket.getOwner().getDisplayName() + " (" + bucket.getOwner().getId() + ")" : null);
					bucketURI.setSize(0l); // buckets has 0 size
					result.add(bucketURI);
				}
			} else { // list objects/subdirs within the bucket/folder
				// prefix: "" for root, "dir/" for subfolder
				String prefix = s3Uri.getPathWithinBucket() == null ? "" : s3Uri.getPathWithinBucket().substring(1); // without leading slash
				ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName(s3Uri.getBucketName()).withDelimiter(S3URIImpl.DELIMITER).withPrefix(prefix);
				ObjectListing objectListing = client.listObjects(listObjectRequest);
				boolean done;
				do {
					String fullpath = s3Uri.getURI();
					String bucketName = s3Uri.getBucketName();
					String onlyHostAndBucket = fullpath.substring(0, fullpath.indexOf(bucketName) + bucketName.length()) + S3URIImpl.DELIMITER;
					
					// directories
					for (String dirEntry: objectListing.getCommonPrefixes()) {
						if (subentires != null && subentires.size() > 0) {
							String entryName = dirEntry;
							if (entryName.endsWith(S3URIImpl.DELIMITER)) entryName = entryName.substring(0, entryName.length() - 1); 
							if (entryName.contains(S3URIImpl.DELIMITER)) entryName = entryName.substring(entryName.lastIndexOf(S3URIImpl.DELIMITER) + 1);
							if (!subentires.contains(entryName + S3URIImpl.DELIMITER)) continue;
						}
						
						S3URIImpl dirURI = new S3URIImpl(onlyHostAndBucket + dirEntry); // it ends with slash
						try {
						    AccessControlList acl = client.getObjectAcl(dirURI.getBucketName(), dirEntry); 
					    	dirURI.setPermissions(getPermissions(acl));
					    } catch (Exception e) {
					    	log.debug("Cannot query ACL of: " + dirURI.getBucketName() + " " + dirEntry);
					    } // not permitted to query bucket/subdir ACL
					    dirURI.setSize(0l); // dirs has 0 size
						result.add(dirURI);
					}
					// objects
					List<S3ObjectSummary> objects = objectListing.getObjectSummaries();
					for (S3ObjectSummary objectSummary: objects) {
					    String key = objectSummary.getKey();
					    if (key.endsWith(S3URIImpl.DELIMITER)) continue; // skip subfolders
					    if (subentires != null && subentires.size() > 0){
						    String entryName = key.contains(S3URIImpl.DELIMITER) ? key.substring(key.lastIndexOf(S3URIImpl.DELIMITER) + 1): key;
					    	if (!subentires.contains(entryName)) continue;
					    }
					    S3URIImpl objectUri = new S3URIImpl(onlyHostAndBucket + key); // key shows full path within the bucket
					    if (objectSummary.getSize() >= 0) objectUri.setSize(objectSummary.getSize());
						if (objectSummary.getLastModified() != null) objectUri.setLastModified(objectSummary.getLastModified().getTime());
						try { objectUri.setPermissions(getPermissions(client.getObjectAcl(s3Uri.getBucketName(), key))); } catch (Exception e) {} // not permitted to query bucket ACL
						if (objectSummary.getOwner() != null && !"".equals(objectSummary.getOwner().getDisplayName())) objectUri.setDetails("Owner: " + objectSummary.getOwner().getDisplayName() + " (" + objectSummary.getOwner().getId() + ")");
						result.add(objectUri);
					}
					// truncated result?
					if (objectListing.isTruncated()) {
						if (result.size() >= 1000) { // if "attributes", return the first n results only
							truncated = true;
							done = true;
						} else {
							log.debug("Truncated, getting more results...");
							objectListing = client.listNextBatchOfObjects(objectListing);
							done = false;
						}
					} else {
						done = true;
					}
				} while(!done);
			}
			
			if (truncated) {
				// indicate truncated
				 S3URIImpl objectUri = new S3URIImpl(s3Uri.getURI() + S3URIImpl.DELIMITER + s3Uri.getBucketName() + s3Uri.getPathWithinBucket() + S3URIImpl.DELIMITER + "..."); 
				 result.add(objectUri);
			}
			return result;
		} 
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); throw new OperationException("Operation exception: '" + e.getMessage() + "'!", e); } 
		finally {
			if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} 
		}
	}
	
	@Override public List<URIBase> list(final URIBase uri, Credentials credentials,	DataAvenueSession session) throws URIException, OperationException, CredentialException {
		S3URIImpl s3Uri = new S3URIImpl(uri.getURI());
		if (s3Uri.getType() != URIBase.URIType.URL && s3Uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory (host/bucket/subdirectory): " + s3Uri.getURI());
		AmazonS3Client client = getAmazonS3Client(s3Uri, credentials, session);
		try {
			List<URIBase> result = new Vector<URIBase>();
			if (s3Uri.getBucketName() == null) { // list bucket names
				for (Bucket bucket: client.listBuckets()) { // uri contains no bucket name
					S3URIImpl bucketURI = new S3URIImpl(s3Uri.getURI() + bucket.getName() + (!bucket.getName().endsWith(S3URIImpl.DELIMITER) ? S3URIImpl.DELIMITER : ""));
					result.add(bucketURI);
				}
			} else { // list objects/dirs within the bucket
				// prefix: "" for bucket root, "dir/" or "/" for subfolder
				final String prefix = (s3Uri.getPathWithinBucket() == null || S3URIImpl.DELIMITER.equals(s3Uri.getPathWithinBucket())) 
						? "" : s3Uri.getPathWithinBucket().substring(1); // without leading slash...
				//ObjectListing objectListing = client.listObjects(s3Uri.getBucketName(), prefix); does not do common prefixes collection
				ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName(s3Uri.getBucketName()).withDelimiter(S3URIImpl.DELIMITER).withPrefix(prefix);
				
				ObjectListing objectListing = client.listObjects(listObjectRequest);
				boolean done;
				do {
					String fullpath = s3Uri.getURI();
					String bucketName = s3Uri.getBucketName();
					String onlyHostAndBucket = fullpath.substring(0, fullpath.indexOf(bucketName) + bucketName.length()) + S3URIImpl.DELIMITER;
					
					// directories
					for (String dirEntry: objectListing.getCommonPrefixes()) {
						S3URIImpl bucketURI = new S3URIImpl(onlyHostAndBucket + dirEntry); // it ends with slash
						// get directory ACL, but there may be no file /dir/ at all (just exists as a part of an object path...)
						result.add(bucketURI);
					}
					// objects
					List<S3ObjectSummary> objects = objectListing.getObjectSummaries();
					for (S3ObjectSummary objectSummary: objects) {
					    String key = objectSummary.getKey();
					    // It will contain the dir entry "prefix/" as a file (if exists). The following line will filter it out.
					    if (key.endsWith(S3URIImpl.DELIMITER)) continue; // skip showing subfolders as files
					    // key may contain folder like: folder/file.txt
					    S3URIImpl objectUri = new S3URIImpl(onlyHostAndBucket + key); // key shows full path within the bucket
						result.add(objectUri);
					}
					// truncated result?
					if (objectListing.isTruncated()) {
						log.debug("Truncated, getting more results...");
						objectListing = client.listNextBatchOfObjects(objectListing);
						done = false;
					} else {
						done = true;
					}
				} while(!done);
			}
			return result;
		} 
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { 
			logAWSException(e);  
			throw new OperationException("Operation exception: '" + e.getMessage() + "'!", e);
		} 
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
	}
	
	
	@Override public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		S3URIImpl s3Uri = new S3URIImpl(uri.getURI());
		if (s3Uri.getType() != URIBase.URIType.URL && s3Uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory (host/bucket/subdirectory)!");
		AmazonS3Client client = getAmazonS3Client(s3Uri, credentials, session);;
		try {
			String base = s3Uri.getURI();
			if (!base.endsWith(S3URIImpl.DELIMITER)) base += S3URIImpl.DELIMITER; // add slash to the end (host or bucket)
	
			if (s3Uri.getPathWithinBucket() == null) { // create bucket
				log.debug("Creating bucket: " + s3Uri.getBucketName()/* + " in region: " + s3Client.getRegionString() + "..."*/);
				String newBucketName = s3Uri.getBucketName().trim();
				// check syntax
				String pattern = "^[a-z0-9][a-z0-9\\.-]{1,61}[a-z0-9]$";
				String patternIP = "(d+\\.){3}d+";
				String patternDots = "\\.\\.";
				if (!newBucketName.matches(pattern)) throw new URIException("Bucket name is invalid! (May consist of lowercase letters, numbers, ., - of length 3-63)");
				if (newBucketName.matches(patternIP)) throw new URIException("Bucket name cannot be an IP address!");
				if (newBucketName.matches(patternDots)) throw new URIException("Bucket name cannot be two dots!");
				
				if (client.doesBucketExist(s3Uri.getBucketName())) throw new OperationException("Bucket already exists or access forbidden!");
				client.createBucket(s3Uri.getBucketName()); // with new Amazon AWS no region must be specified (old usedendpoint -> amazon region mapping, and threw exception on non-amazon endpoint) 
			} else { // create folder
				client.putObject(s3Uri.getBucketName(), s3Uri.getPathWithinBucket().substring(1), new ByteArrayInputStream(new byte[0]), null);
			}
		} 
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); throw new OperationException("Operation exception: '" + e.getMessage() + "'!", e); }
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
	}

	@Override public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		S3URIImpl s3Uri = new S3URIImpl(uri.getURI());
		if (s3Uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory (host/bucket/subdirectory)!");
		AmazonS3Client client = getAmazonS3Client(s3Uri, credentials, session);
		try {
			
			if (s3Uri.getPathWithinBucket() == null) { // delete bucket
				log.debug("Deleting bucket: " + s3Uri.getBucketName() + "...");
				if (!client.doesBucketExist(s3Uri.getBucketName())) throw new OperationException("Bucket does not exist!");
				// delete all objects (+versions) in bucket
				
				// delete all objects
				ObjectListing objectListing;
				List<KeyVersion> keysToDelete = new ArrayList<KeyVersion>();;
				do {
					objectListing = client.listObjects(s3Uri.getBucketName());
					for (S3ObjectSummary objectSummary: objectListing.getObjectSummaries()) 
						keysToDelete.add(new KeyVersion(objectSummary.getKey()));
					
					if (keysToDelete.size() > 0) { 
						DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(s3Uri.getBucketName()).withKeys(keysToDelete);
						try {
						    DeleteObjectsResult delObjRes = client.deleteObjects(multiObjectDeleteRequest);
						    log.debug(delObjRes.getDeletedObjects().size() + " items deleted");
						} catch (MultiObjectDeleteException e) {
							log.debug("Successfully deleted: " + e.getDeletedObjects().size());
							log.debug("Could not delete: " + e.getErrors().size());
							log.error("MultiObjectDeleteException exception: " + e.getMessage());
							// for (DeleteError deleteError : e.getErrors()) log.debug(deleteError.getKey() + " " + deleteError.getCode() + " " + deleteError.getMessage());
						    throw new OperationException("Could not delete " + e.getErrors().size() + " items!", e);
						} catch (AmazonS3Exception x) {
							log.error("AmazonS3Exception: " + x.getMessage());
						}
						keysToDelete.clear();
					}
					
				} while(objectListing.isTruncated());
				
				client.deleteBucket(s3Uri.getBucketName()); // throws exception if not empty
			} else { // delete folder
				String prefix = s3Uri.getPathWithinBucket().substring(1);
				ObjectListing objectListing = client.listObjects(s3Uri.getBucketName(), prefix);
				List<S3ObjectSummary> objects = objectListing.getObjectSummaries();
				// empty folder
				if (objects.size() == 0) {
					client.deleteObject(new DeleteObjectRequest(s3Uri.getBucketName(), s3Uri.getPathWithinBucket().substring(1)));
				} else {
					// what if versioned?
					List<KeyVersion> keys = new ArrayList<KeyVersion>();
					for (S3ObjectSummary objectSummary: objects) keys.add(new KeyVersion(objectSummary.getKey()));
					DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(s3Uri.getBucketName()).withKeys(keys);
					try {
					    DeleteObjectsResult delObjRes = client.deleteObjects(multiObjectDeleteRequest);
					    log.debug(delObjRes.getDeletedObjects().size() + " items deleted");
					} catch (MultiObjectDeleteException e) {
						log.debug("Deleted: " + e.getDeletedObjects().size());
						log.debug("Could not delete: " + e.getErrors().size());
						log.error("MultiObjectDeleteException exception: " + e.getMessage());
						for (DeleteError deleteError : e.getErrors()) log.debug(deleteError.getKey() + " " + deleteError.getCode() + " " + deleteError.getMessage());
					    throw new OperationException("Could not delete " + e.getErrors().size() + " items!", e);
					}
				}
			}
		} 
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); throw new OperationException("Operation exception: '" + e.getMessage() + "'!", e); } 
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
	}

	@Override public void delete(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		S3URIImpl s3Uri = new S3URIImpl(uri.getURI());
		if (s3Uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file entry!");
		if (s3Uri.getBucketName() == null) throw new URIException("No bucket name!");
		if (s3Uri.getPathWithinBucket() == null) throw new URIException("No path to file!");
		AmazonS3Client client = getAmazonS3Client(s3Uri, credentials, session);
		try {		
			client.deleteObject(new DeleteObjectRequest(s3Uri.getBucketName(), s3Uri.getPathWithinBucket().substring(1))); // omit first slash
			// NOTE: no exception thrown if the object to be deleted does not exist!
		}
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); throw new OperationException("Operation exception: '" + e.getMessage() + "'!", e); } 
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
	}

	private String getPermissions(AccessControlList acl) {
		String ownerPermissions = "---";
		String groupPermissions = "---";;
		String othersPermissions = "---";;
		
		@SuppressWarnings("deprecation")
		Set<Grant> grants = acl.getGrants();
		if (grants != null) {
			for (Grant grant: grants) {
		
				if (grant.getGrantee() instanceof CanonicalGrantee) { // premission for a given user
					//System.out.println("CanonicalGrantee");
					if (acl.getOwner().getId().equals(grant.getGrantee().getIdentifier())) {
						StringBuilder sb = new StringBuilder();
						if (grant.getPermission() == Permission.FullControl) {
							sb.append("rw-");
						}
						else {
							sb.append(grant.getPermission() == Permission.Read ? "r" : "-");
							sb.append(grant.getPermission() == Permission.Write ? "w" : "-");
							sb.append("-");
						}
						ownerPermissions = sb.toString(); 
					}
				} else if (grant.getGrantee() instanceof GroupGrantee) {
					if ((GroupGrantee)grant.getGrantee() == GroupGrantee.AuthenticatedUsers) {
						StringBuilder sb = new StringBuilder();
						if (grant.getPermission() == Permission.FullControl) sb.append("rw-");
						else {
							sb.append(grant.getPermission() == Permission.Read ? "r" : "-");
							sb.append(grant.getPermission() == Permission.Write ? "w" : "-");
							sb.append("-");
						}
						groupPermissions = sb.toString(); 
					} else if ((GroupGrantee)grant.getGrantee() == GroupGrantee.AllUsers) {
						StringBuilder sb = new StringBuilder();
						if (grant.getPermission() == Permission.FullControl) sb.append("rw-");
						else {
							sb.append(grant.getPermission() == Permission.Read ? "r" : "-");
							sb.append(grant.getPermission() == Permission.Write ? "w" : "-");
							sb.append("-");
						}
						othersPermissions = sb.toString(); 
					}
				} else if (grant.getGrantee() instanceof EmailAddressGrantee) {
				} else {
					log.warn("Unknown grantee type: " + grant.getGrantee().getClass().getName());
				}
			}
		} // end if (grants != null) {
		return ownerPermissions + groupPermissions + othersPermissions;
	}
	
	@Override public void permissions(final URIBase uri, final Credentials credentials, final DataAvenueSession session, final String permissionsString) throws URIException, OperationException, CredentialException {
		if (permissionsString == null || permissionsString.length() != 9) throw new OperationException("Invalid permissions string");
		S3URIImpl s3Uri = new S3URIImpl(uri.getURI());
		AmazonS3Client client = getAmazonS3Client(s3Uri, credentials, session);;
		try {
			// -rw-rw-rw
			// 012345678
			//boolean ownerRead = permissionsString.charAt(1) == 'r'; // cannot be changed
			//boolean ownerWrite = permissionsString.charAt(2) == 'w'; // cannot be changed
			boolean groupRead = permissionsString.charAt(4) == 'r';
			boolean groupWrite = permissionsString.charAt(5) == 'w';
			boolean otherRead = permissionsString.charAt(7) == 'r';
			boolean otherWrite = permissionsString.charAt(8) == 'w';
			
			if (s3Uri.getPathWithinBucket() == null && s3Uri.getBucketName() != null) { //  set bucket ACL

				AccessControlList acl = client.getBucketAcl(s3Uri.getBucketName());
	            
				// cannot modify owner grants
				
	            acl.revokeAllPermissions(GroupGrantee.AuthenticatedUsers);
				if (groupRead) acl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);
				if (groupWrite) acl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Write);
				
	            acl.revokeAllPermissions(GroupGrantee.AllUsers);
				if (otherRead) acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
				if (otherWrite) acl.grantPermission(GroupGrantee.AllUsers, Permission.Write);
				
				client.setBucketAcl(s3Uri.getBucketName(), acl);

			} else { // set object ACL
				
				AccessControlList acl = client.getObjectAcl(s3Uri.getBucketName(), s3Uri.getPathWithinBucket().substring(1));
	            
				// cannot modify owner grants
				
	            acl.revokeAllPermissions(GroupGrantee.AuthenticatedUsers);
				if (groupRead) acl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);
				if (groupWrite) acl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Write);
				
	            acl.revokeAllPermissions(GroupGrantee.AllUsers);
				if (otherRead) acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
				if (otherWrite) acl.grantPermission(GroupGrantee.AllUsers, Permission.Write);
				
				client.setObjectAcl(s3Uri.getBucketName(), s3Uri.getPathWithinBucket().substring(1), acl);
			}
		} 
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); throw new OperationException(e); } 
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
	}
	
	@Override public void rename(URIBase uri, String newName, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		// copy, then delete (folder copy copies all objects within the renamed folder)
		S3URIImpl src = new S3URIImpl(uri.getURI());
		// asserts: copy file or folder (bucket copy not allowed), newName cannot contain slash
		if (src.getBucketName() == null) throw new URIException("No bucket name provided!");
		if (src.getType() != URIBase.URIType.FILE && src.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a file or a directory entry!");
		if (newName.contains(S3URIImpl.DELIMITER)) throw new URIException("New name cannot contain '" + S3URIImpl.DELIMITER + "' character!");
		AmazonS3Client client = getAmazonS3Client(src, credentials, session);;
		try {
			String bucketName = src.getBucketName();
			
			if (src.getType() == URIBase.URIType.FILE) { // file rename
				// copy with a new name
				String newKey = src.getPathWithinBucket().substring(1, src.getPathWithinBucket().lastIndexOf(S3URIImpl.DELIMITER) + 1) + newName;
				log.debug("rename: " +  src.getPathWithinBucket().substring(1) + " -> " + newKey);
				
				
				try { // check destination exists
					client.getObjectMetadata(src.getBucketName(), newKey); // if no exception thrown, new file exists
					throw new OperationException("A file with the new name already exists!");
				} catch (AmazonS3Exception e) {} // else OK

				// get and copy ACL of the source? AccessControlList grants = getObjectAcl(src.getBucketName(), newKey);
				CopyObjectRequest copyObjRequest = new CopyObjectRequest(src.getBucketName(), src.getPathWithinBucket().substring(1), src.getBucketName(), newKey)/*.withAccessControlList(grants)*/;
				CopyObjectResult result = null;
				try { result = client.copyObject(copyObjRequest); } 
				catch (AmazonS3Exception e) { throw new OperationException("Source file does not exist!"); }
				
				if (result == null) throw new OperationException("Couldn't copy file!");
				else client.deleteObject(new DeleteObjectRequest(src.getBucketName(), src.getPathWithinBucket().substring(1))); // delete original object, omit first slash
				
			} else { // directory rename

				if (src.getPathWithinBucket() == null) { // bucket rename
					
					// create new bane 
					String newBucketName = newName.trim();
					// check syntax
					String pattern = "^[a-z0-9][a-z0-9\\.-]{1,61}[a-z0-9]$";
					String patternIP = "(d+\\.){3}d+";
					String patternDots = "\\.\\.";
					if (!newBucketName.matches(pattern)) throw new URIException("Bucket name is invalid (may consist of lowercase letters, numbers, ., -)!");
					if (newBucketName.matches(patternIP)) throw new URIException("Bucket name cannot be an IP address!");
					if (newBucketName.matches(patternDots)) throw new URIException("Bucket name cannot be two dots!");
					if (client.doesBucketExist(newBucketName)) throw new OperationException("A bucket with the same name already exists!");
					client.createBucket(newBucketName/*, s3Client.getRegionString()*/); // new Amazon SDK works without region 

					// move all files to new bucket
					ObjectListing objectListing;
					List<KeyVersion> keysToDelete = new ArrayList<KeyVersion>();;
					do {
						objectListing = client.listObjects(src.getBucketName());
						for (S3ObjectSummary objectSummary: objectListing.getObjectSummaries()) {
							client.copyObject(src.getBucketName(), objectSummary.getKey(), newBucketName, objectSummary.getKey());
							keysToDelete.add(new KeyVersion(objectSummary.getKey()));
						}
					} while(objectListing.isTruncated());
					DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(src.getBucketName()).withKeys(keysToDelete);
					try {
					    DeleteObjectsResult delObjRes = client.deleteObjects(multiObjectDeleteRequest);
					    log.debug(delObjRes.getDeletedObjects().size() + " items deleted");
					} catch (MultiObjectDeleteException e) {
						log.debug("Deleted: " + e.getDeletedObjects().size());
						log.debug("Could not delete: " + e.getErrors().size());
						log.error("MultiObjectDeleteException exception: " + e.getMessage());
						for (DeleteError deleteError : e.getErrors()) log.debug(deleteError.getKey() + " " + deleteError.getCode() + " " + deleteError.getMessage());
					    throw new OperationException("Could not delete " + e.getErrors().size() + " items!", e);
					}

					client.deleteBucket(src.getBucketName()); // throws exception if not empty

				} else { // rename subdir

					
					assert src.getPathWithinBucket().length() > 2; // at least: /d/ 
					String dirToRename = src.getPathWithinBucket().substring(1, src.getPathWithinBucket().length() - 1); // cut leading-trailing slash
					String newDirName = !dirToRename.contains(S3URIImpl.DELIMITER) ? newName : dirToRename.substring(0, dirToRename.lastIndexOf(S3URIImpl.DELIMITER) + 1) + newName;
					
					// check any target already exists, throw Exception if does...
					ListObjectsRequest listObjectRequest = new ListObjectsRequest().
							withBucketName(bucketName).
							withDelimiter(S3URIImpl.DELIMITER).
							withPrefix(newDirName + S3URIImpl.DELIMITER);
					ObjectListing objectListing = client.listObjects(listObjectRequest);
					if (objectListing.getCommonPrefixes().size() > 0 || objectListing.getObjectSummaries().size() > 0) 
						throw new OperationException("Target directory already exists!"); 
					
					boolean done; // rename objects one-by-one
					do {
						// list all entries starting with old dir name (including the old dir entry)
						String prefix = dirToRename + S3URIImpl.DELIMITER;
						objectListing = client.listObjects(bucketName,	prefix);
						// for all entries
						List<S3ObjectSummary> objects = objectListing.getObjectSummaries();
						if (objects.size() == 0) { throw new OperationException("Source directory to be renamed not found!"); }
						else for (S3ObjectSummary objectSummary: objects) {
						    String oldKey = objectSummary.getKey();
						    if (!oldKey.contains(prefix)) { log.error("App error"); continue; } // silent failover, should not happen
						    String oldPostfix = oldKey.substring(prefix.length()); // cut old prefix
						    String newKey;
						    if (oldPostfix.length() == 0) newKey = newDirName + S3URIImpl.DELIMITER; // the dir entry itself
						    else newKey = newDirName + S3URIImpl.DELIMITER + oldPostfix;
						    log.trace("entry to be renamed: " + oldKey  + " -> " + newKey);
						    // do the copy
							CopyObjectRequest copyObjRequest;
							// get and copy ACL of the source? 
							// AccessControlList grants = getObjectAcl(src.getBucketName(), newKey);
							copyObjRequest = new CopyObjectRequest(bucketName, oldKey, bucketName, newKey);
							CopyObjectResult result = null;
							try { result = client.copyObject(copyObjRequest); } 
							catch (AmazonS3Exception e) { throw new OperationException("Source file does not exist!"); }
							// do the delete, if copy succeeded
							if (result == null) throw new OperationException("Couldn't rename file (copy)!");
							else client.deleteObject(new DeleteObjectRequest(bucketName, oldKey)); 
						}
						// truncated?
						if (objectListing.isTruncated()) {
							objectListing = client.listNextBatchOfObjects(objectListing);
							done = false;
						} else {
							done = true;
						}
					} while (!done);
				} // end else bucket rename
			} // end if dir rename
		}
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); throw new OperationException(e); } 
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
	}

	@Override public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		try {
			S3URIImpl src = new S3URIImpl(uri.getURI()); // URI must be a file
			if (src.getBucketName() == null) throw new URIException("No bucket name!");
			if (src.getType() != URIBase.URIType.FILE || src.getPathWithinBucket() == null) throw new URIException("URI is not a file!");
				
			AmazonS3Client client = getAmazonS3Client(src, credentials, session);

			String bucketName = src.getBucketName();
	
			S3Object object = client.getObject(new GetObjectRequest(bucketName, src.getPathWithinBucket().substring(1)));
			if (object == null) throw new OperationException("Amazon S3 constraint violation!"); // should not happen
			return new InputStreamWrapper(object.getObjectContent(), session, client);
		}
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); throw new OperationException("Cannot open resource input stream!",e); }
	}

	/*
	0B (unknown content length): multipart upload part size: 10MiB, threads: 2
	(0B-10MB: putobject upload, NOTE: this is handled separately, see writeFromInputStream 
	[10MB-100MB): multipart upload part size: 5MiB, threads: 4 (put over presigned url is slower)
	[100MB-50GB): multipart upload part size: 5MiB, threads: 4
	[50GB-100GB): multipart upload part size: 10MiB, threads: 4
	[100GB-500GB): multipart upload part size: 50MiB, threads: 2
	[50GB-1TB): multipart upload part size: 100MiB, threads: 2
	[1TB-5TB]: multipart upload part size: 500MiB, threads: 2
	 */
	public static OutputStream getOutputStreamForSize(AmazonS3Client client, String bucketName, String objectName, long size) throws OperationException {
		log.trace("Getting s3 output stream for content length: " + size + "...");
		final long /*_100MB = 100000000l,*/ _50GB = 50000000000l, _100GB = 100000000000l, _500GB = 500000000000l, _1TB = 1000000000000l, _5TB = 5000000000000l;
		final int _5MiB = 5 * 1024 * 1024, _10MiB = 10 * 1024 * 1024, _50MiB = 50 * 1024 * 1024, _100MiB = 100 * 1024 * 1024, _500MiB = 500 * 1024 * 1024;
		if (size < 0) { // unknown content length, return default stream up to 100GB upload
			return new ThreadedMultipartUploadOutputStream(client, bucketName, objectName, _10MiB, 2, size); // 10MB multipart upload, 2 threads
		} else if (size == 0) {
			throw new OperationException("Cannot use multi-part upload for 0-long content"); 
//		} else if (size < _100MB) { // up to 100MB use presigned urls
//			return new ThreadedMultipartUploadOutputStream(client, bucketName, objectName, _5MiB, size); // 5MB multipart upload
		} else if (size <= _50GB) { // up to 50GB use multipart upload with 10MB part size  
			return new ThreadedMultipartUploadOutputStream(client, bucketName, objectName, _5MiB, 4, size); // 5MB multipart upload
		} else if (size <= _100GB) { // up to 100GB use  multipart upload with 10MB part size
			return new ThreadedMultipartUploadOutputStream(client, bucketName, objectName, _10MiB, 4, size); // 10MB multipart upload
		} else if (size <= _500GB) { 
			return new ThreadedMultipartUploadOutputStream(client, bucketName, objectName, _50MiB, 2, size); // 50MB multipart upload, 2 threads
		} else if (size <= _1TB) { 
			return new ThreadedMultipartUploadOutputStream(client, bucketName, objectName, _100MiB, 2, size); // 100MB multipart upload, 2 threads
		} else if (size <= _5TB) { 
			return new ThreadedMultipartUploadOutputStream(client, bucketName, objectName, _500MiB, 2, size); // 500MB multipart upload, 2 threads
		} else {
			if (client != null) { try { client.shutdown(); } catch (Throwable x) {} }
			throw new OperationException("S3 objects can be of size up to 5 terabytes (TB)");
		}
	}
	
	@Override public OutputStream getOutputStream(URIBase uri, Credentials credentials,	DataAvenueSession session, long size) throws URIException, OperationException, CredentialException {
		AmazonS3Client client = null;
		try {
			// file
			S3URIImpl src = new S3URIImpl(uri.getURI());
			if (src.getBucketName() == null) throw new URIException("No bucket name!");
			if (src.getType() != URIBase.URIType.FILE || src.getPathWithinBucket() == null) throw new URIException("URI is not a file!");
				
			client = getAmazonS3Client(src, credentials, session);
			String bucketName = src.getBucketName();
			
			return new OutputStreamWrapper(getOutputStreamForSize(client, bucketName, src.getPathWithinBucket().substring(1), size), session, client);
		}
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			if (client != null) { try { client.shutdown(); } catch (Throwable x) {} }
			throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { 
			logAWSException(e);
			if (client != null) { try { client.shutdown(); } catch (Throwable x) {} }
			throw new OperationException("Cannot open resource input stream!",e); }
	}
	
	@Override public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session, InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException, OperationNotSupportedException {
    	// use up to 100MB use putobject
    	if (contentLength < 0 || contentLength > PutObjectOutputStream.PUT_OBJECT_LIMIT) throw new OperationNotSupportedException("Operation not supported for unkown file size or above " + PutObjectOutputStream.PUT_OBJECT_LIMIT + " bytes");
    	
		AmazonS3Client client = null;
		try {
			// file
			S3URIImpl src = new S3URIImpl(uri.getURI());
			if (src.getBucketName() == null) throw new URIException("No bucket name!");
			if (src.getType() != URIBase.URIType.FILE || src.getPathWithinBucket() == null) throw new URIException("URI is not a file!");
				
			client = getAmazonS3Client(src, credentials, session);
			PutObjectOutputStream.writeInputStream(client, src.getBucketName(), src.getPathWithinBucket().substring(1), inputStream, contentLength);
		}
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			if (client != null) { try { client.shutdown(); } catch (Throwable x) {} }
			throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { 
			logAWSException(e);
			if (client != null) { try { client.shutdown(); } catch (Throwable x) {} }
			throw new OperationException("Cannot open resource input stream!",e); }
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
    }
	
	@Override public String copy(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		return copyOrMove(fromUri, fromCredentials, toUri, toCredentials, overwrite, monitor, false);
	}

	@Override public String move(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		return copyOrMove(fromUri, fromCredentials, toUri, toCredentials, overwrite, monitor, true);
	}

	private String copyOrMove(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor, boolean isMove) throws URIException, OperationException, CredentialException {
		S3URIImpl source = new S3URIImpl(fromUri.getURI());
		if (source.getBucketName() == null) throw new URIException("No bucket name!");
		if (source.getType() != URIBase.URIType.FILE || source.getPathWithinBucket() == null) throw new URIException("Source URI is not a file!");

		S3URIImpl target = new S3URIImpl(toUri.getURI());
		if (target.getBucketName() == null) throw new URIException("No bucket name!");
		if (target.getType() != URIBase.URIType.FILE || target.getPathWithinBucket() == null) throw new URIException("Target URI is not a file!");

		UUID id = UUID.randomUUID();
		S3CopyTask task = new S3CopyTask(source, fromCredentials, target, toCredentials, monitor, isMove, overwrite, S3CopyTaskRegistry.getInstance(), id);
		monitor.scheduled();
		S3CopyTaskRegistry.getInstance().submit(id, task);
		
		return id.toString();
	}
	
	@Override public void cancel(String id) throws TaskIdException, OperationException {
		S3CopyTaskRegistry.getInstance().cancel(id);
	}

	@Override public long getFileSize(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		S3URIImpl src = new S3URIImpl(uri.getURI());
		if (src.getBucketName() == null) throw new URIException("No bucket name!");
		if (src.getType() != URIBase.URIType.FILE || src.getPathWithinBucket() == null) throw new URIException("URI is not a file!");
			
		AmazonS3Client client = getAmazonS3Client(src, credentials, session);
		try {
			String bucketName = src.getBucketName();

			try {
				
				ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName(bucketName).withDelimiter(S3URIImpl.DELIMITER).withPrefix(src.getPathWithinBucket().substring(1));
				ObjectListing objectListing = client.listObjects(listObjectRequest); // returns 1 long list
				if (objectListing != null) for (S3ObjectSummary objectSummary: objectListing.getObjectSummaries()) {
				    return objectSummary.getSize();
				}		
				throw new OperationException("Object not found: " + uri.getURI());
				/*
				// NOTE does not work on Amazon
				log.debug("getObjectMetadata " + bucketName + " " + src.getPathWithinBucket().substring(1));
				log.trace("UserID: " + credentials.getCredentialAttribute("UserID") + " " + client);
				ObjectMetadata metaData = client.getObjectMetadata(bucketName, src.getPathWithinBucket().substring(1));
				if (metaData == null) {
					log.trace("null metadata for object: " + uri.getURI());
					throw new OperationException("Cannot read metadata of file: " + uri.getURI());
				}
				return metaData.getContentLength();
				*/
			} catch (AmazonS3Exception e) { 
				if ("NoSuchKey".equals(e.getErrorCode())) throw new OperationException("Object not found: " + uri.getURI(), e);
				else throw new OperationException("Cannot read metadata of file: " + e.getErrorCode() + " " + e.getAdditionalDetails(), e);
			}
		}
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); throw new OperationException("Cannot open resource input stream!",e); }
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
	}

	// test if object is readable
	@Override public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		S3URIImpl src = new S3URIImpl(uri.getURI());
		if (src.getBucketName() == null) throw new URIException("No bucket name!");
		if (src.getType() != URIBase.URIType.FILE || src.getPathWithinBucket() == null) throw new URIException("URI is not a file!");
		AmazonS3Client client = getAmazonS3Client(src, credentials, session);
		try {
			String bucketName = src.getBucketName();

			S3Object object = client.getObject(new GetObjectRequest(bucketName, src.getPathWithinBucket().substring(1)));
			if (object == null) return false;
			else return true;
			/*
			NOTE: does not work on Amazon 
			// if we can read the metadata, I can read the file too
			try { client.getObjectMetadata(bucketName, src.getPathWithinBucket().substring(1)); } 
			catch (AmazonS3Exception e) { return false; }
			*/
		}
		catch (AmazonServiceException e) {
//			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
//			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e);
			return false;
		}
		catch (AmazonClientException e) { 
			return false; 
		}
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
	}

	@Override public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		// if the object exists, check permissions, otherwise read bucket permissions
		S3URIImpl src = new S3URIImpl(uri.getURI());
		if (src.getBucketName() == null) throw new URIException("No bucket name!");
		if (src.getType() != URIBase.URIType.FILE || src.getPathWithinBucket() == null) throw new URIException("URI is not a file!");
		AmazonS3Client client = getAmazonS3Client(src, credentials, session);;
		try {
			String bucketName = src.getBucketName();

			// try to put a 0-byte-long file, if succeeds you can write the file 
			try { client.putObject(bucketName, src.getPathWithinBucket().substring(1), new ByteArrayInputStream(new byte[0]), null); } 
			catch (AmazonS3Exception e) { return false; }
			return true;
		}
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); return false; }
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
	}

	public String createDirectURL(URIBase uri, Credentials credentials, boolean read, int lifetime, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		// single file upload using generate pre-signed URL
		// with a single PUT operation you can upload objects up to 5 GB in size (can range in size from 1 byte to 5 terabytes) 
		S3URIImpl src = new S3URIImpl(uri.getURI());
		if (src.getBucketName() == null) throw new URIException("No bucket name!");
		if (src.getType() != URIBase.URIType.FILE || src.getPathWithinBucket() == null) throw new URIException("URI is not a file!");
		AmazonS3Client client = getAmazonS3Client(src, credentials, session);

		try {
			String bucketName = src.getBucketName();
		
			GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, src.getPathWithinBucket().substring(1));
			generatePresignedUrlRequest.setMethod(read ? HttpMethod.GET : HttpMethod.PUT); 
			long expiration = System.currentTimeMillis() + 1000 * 60 * lifetime; 
			generatePresignedUrlRequest.setExpiration(new Date(expiration));
			URL url = client.generatePresignedUrl(generatePresignedUrlRequest);
			return url.toString();
		}
		catch (AmazonServiceException e) {
			logAWSException(e); 
			if (e.getStatusCode() == 301) throw new OperationException("Bucket is not reachable from this endpoint!", e);
			else throw new OperationException("Operation exception: '" + e.getErrorCode() + "'!", e); 
		}
		catch (AmazonClientException e) { logAWSException(e); throw new OperationException(e); }
		finally { if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} }
	}
	
	@Override
	public void shutDown() {}
	
	private void logAWSException(AmazonClientException e) {
		log.debug("Exception: " + e.getMessage(), e);
		if (e instanceof AmazonServiceException) {
			AmazonServiceException ase = (AmazonServiceException) e;
	        log.trace("Caught an AmazonServiceException:");
	        log.trace("  Error Message:    " + ase.getMessage());
	        log.trace("  HTTP Status Code: " + ase.getStatusCode());
	        log.trace("  AWS Error Code:   " + ase.getErrorCode());
	        log.trace("  Error Type:       " + ase.getErrorType());
	        log.trace("  Request ID:       " + ase.getRequestId());
		} 
	}	
}