package hu.sztaki.lpds.dataavenue.rest;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

// WARNING: dont compile with core war because dependency jersey 1.19 fcks up all
// FIXME: rewrite to jersey 2.17 API!!!!!!!!!!!!!
@Deprecated
public class DataAvenueRESTClient {
	
	public static final String HTTP_HEADER_KEY = "X-Key";
	public static final String HTTP_HEADER_URI = "X-URI";
	public static final String HTTP_HEADER_USERNAME = "X-Username";
	public static final String JSAGA_USERNAME_KEY = "UserID";
	public static final String HTTP_HEADER_PASSWORD = "X-Password";
	public static final String JSAGA_PASSWORD_KEY = "UserPass";
	public static final String JSAGA_USERPASS_TYPE = "UserPass";
	public static final String JSAGA_PROXY = "Proxy";
	public static final String JSAGA_PROXY_TYPE = "Globus";
	public static final String JSAGA_CREDENTIALS_TYPE = "Type";
	public static final String HTTP_HEADER_PROXY = "X-Proxy";
	public static final String HTTP_HEADER_PROXY_TYPE = "X-Credential-Proxy-Type"; // e.g., X-Proxy-Type: VOMS
	public static final String HTTP_HEADER_VO = "X-Credential-VO"; 
	public static final String HTTP_HEADER_CERTIFICATE = "X-Credential-Certificate";
	public static final String HTTP_HEADER_RESOURCE = "X-Credential-Resource"; // iRODS resource
	public static final String HTTP_HEADER_ZONE = "X-Credential-Zone"; // iRODS source
	public static final String HTTP_HEADER_REDIRECT = "X-Accept-Redirects"; // yes | no
	public static final String HTTP_HEADER_USE_SESSION = "X-Use-Session"; // yes (any value) | no (if header absent)

	public static final String ALIAS_LIFETIME =  "lifetime";
	public static final String ALIAS_READ = "read";
	public static final String ALIAS_REDIRECT = "redirect";
	public static final String ALIAS_ARCHIVE = "archive";
	
	public static final String JSON_CREDENTIALS = "credentials";
	public static final String JSON_TARGET = "target";
	public static final String JSON_MOVE = "move";
	
	private final String url, key; 
	private final boolean secure;
	private final boolean allowRedirects;
	
	public static class Builder {
		private final String key; // required
		private final String url; // required
		private String dnsServer; // null
		private boolean secure = false; // default
		private String serviceName = "lb.service.dataavenue.consul"; // default
		private boolean allowRedirects = false; // default
		
		/**
		 * Creates a rest client builder with the specified access key (required parameter)
		 * @param key Data Avenue service REST URL
		 * @param key Access key to Data Avenue services
		 */
		public Builder(final String url, final String key) {
			this.key = key;
			this.url = url;
		}
		
		/**
		 * Sets DNS for Data Avenue discovery (e.g., Consul registry of load balancers).
		 * On successful resolution, it will replace host in field "url".
		 * @param dnsServer DNS server containing service registry
		 * @return Builder
		 */
		public Builder withDNS(String dnsServer) {
			this.dnsServer = dnsServer; 
			return this;
		}
		
		/**
		 * Sets service name to resolve by DNS (default: "lb.service.dataavenue.consul")
		 * @param serviceName Service/host name to resolve
		 * @return Builder
		 */
		public Builder withServiceName(String serviceName) {
			this.serviceName = serviceName; 
			return this;
		}
		
		/**
		 * Sets secure client with hostname verification (default: false)
		 * @param secure HTTPS hostname verification is on (true) or disabled (false)
		 * @return Builder
		 */
		public Builder setSecure(boolean secure) {
			this.secure = secure; 
			return this;
		}
		
		/**
		 * Allows redirects (use of S3 pre-signed URLs, default: false)
		 * @param allowRedirects false: disables redirects
		 * @return Builder
		 */
		public Builder allowRedirects(boolean allowRedirects) {
			this.allowRedirects = allowRedirects; 
			return this;
		}
		
		/**
		 * Builds the client
		 * @return DataAvenueRESTClient
		 * @throws UnknownHostException If no rest URL is specified or DNS resolution failed
		 */
		public DataAvenueRESTClient build() throws UnknownHostException, MalformedURLException {
			return new DataAvenueRESTClient(this);
		}
	}
	
	private DataAvenueRESTClient(Builder builder) throws UnknownHostException, MalformedURLException {
		key = builder.key;
		secure = builder.secure;
		allowRedirects = builder.allowRedirects;
		if (builder.dnsServer != null) {
			URL url2change = new URL(builder.url); 
			String ip = resolveURL2(builder.dnsServer, builder.serviceName);
			url = url2change.getProtocol() + "://" + ip + (url2change.getPort() != -1 ? ":" + url2change.getPort() : "") + url2change.getPath();
		} else url = builder.url;
	}
	
	/**
	 * Creates a (secure) Data Avenue REST client
	 * @param restURL URL of the REST service, e.g. https://data-avenue.hu/blacktop/rest
	 * @param key Access key to Data Avenue services
	 */
//	public DataAvenueRESTClient(final String restURL, final String key) {
//		this(restURL, key, true);
//	}
	
	/**
	 * Creates an secure/unsecure Data Avenue REST client
	 * @param url URL of the REST service, e.g. https://data-avenue.hu/blacktop/rest
	 * @param key Access key to Data Avenue services
	 * @param secure If false, no host verification made on HTTPS connections 
	 */
//	public DataAvenueRESTClient(final String restURL, final String key, final boolean secure) {
//		this.restURL = restURL;
//		this.key = key;
//		this.secure = secure;
//	}
	
	private Client httpClient = null;
	private Client getHttpClient() {
		if (httpClient == null) {
			httpClient = secure ? Client.create() : Client.create(configureClient());
			httpClient.setFollowRedirects(allowRedirects);
			httpClient.setChunkedEncodingSize(1024);
		}
		return httpClient;
	}

	// add map key, value pairs as HTTP header name, values
	private void addMapAsHttpHeader(final com.sun.jersey.api.client.WebResource.Builder b, final Map<String, String> map) {
		if (map == null) return;
		for (Map.Entry<String, String> entry: map.entrySet()) 
			b.header(entry.getKey(), entry.getValue());
	}
	
	// add entries corresponding to JSaga key names, determine type if absent 
	private void addJasagaKeysToMap(final JSONObject jsonObject) {
		if (jsonObject == null) return;
		if (!jsonObject.has(JSON_CREDENTIALS)) return;
		try { 
			JSONObject map = jsonObject.getJSONObject(JSON_CREDENTIALS);
			addJasagaKeysToMap(map, HTTP_HEADER_USERNAME, JSAGA_USERNAME_KEY);
			addJasagaKeysToMap(map, HTTP_HEADER_PASSWORD, JSAGA_PASSWORD_KEY);
			if (!map.has(JSAGA_CREDENTIALS_TYPE)) {
				if (map.has(JSAGA_PASSWORD_KEY)) map.put(JSAGA_CREDENTIALS_TYPE, JSAGA_USERPASS_TYPE);
				else if (map.has(JSAGA_PROXY)) map.put(JSAGA_CREDENTIALS_TYPE, JSAGA_PROXY_TYPE); 
			}
		} catch (JSONException e) {}
	}

	private void addJasagaKeysToMap(JSONObject map, final String key, final String newKey) {
		if (map.has(key) && !map.has(newKey)) map.put(newKey, map.get(key));
	}
	
	/**
	 * Gets the list of directory entries
	 * @param uri URI of the remote directory
	 * @param creds Credentials for the remote storage
	 * @return JSONArray of subentries (files, subdirectories)
	 * @throws Exception On any error
	 */
	public JSONArray list(final String uri, final Map<String, String> creds) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/directory");
			com.sun.jersey.api.client.WebResource.Builder b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).accept(MediaType.APPLICATION_JSON);
			addMapAsHttpHeader(b, creds);
			String response = b.get(String.class);
	   		try { return new JSONArray(new JSONTokener(response)); }
	   		catch (JSONException e) { throw new Exception("JSON syntax error in response", e); }
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Creates a new directory
	 * @param uri URI of a remote directory
	 * @param creds Credentials for the remote storage
	 * @throws Exception On any error
	 */
	public void mkdir(final String uri, final Map<String, String> creds) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/directory");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri);
			addMapAsHttpHeader(b, creds);
			b.post(String.class);
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Removes a directory (recursively)
	 * @param uri URI of the remote directory
	 * @param creds Credentials for the remote storage
	 * @throws Exception On any error
	 */
	public void rmdir(final String uri, final Map<String, String> creds) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/directory");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri);
			addMapAsHttpHeader(b, creds);
			b.delete();
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}

	}
	
	/**
	 * Gets file or directory attributes
	 * @param uri URI of the remote file or directory
	 * @param creds Credentials for the remote storage
	 * @return JSONObject of attributes
	 * @throws Exception On any error
	 */
	public JSONObject attributes(final String uri, final Map<String, String> creds) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/attributes");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).accept(MediaType.APPLICATION_JSON);
			addMapAsHttpHeader(b, creds);
			String response = b.get(String.class);
			try { return new JSONObject(new JSONTokener(response)); }
	    	catch (JSONException e) { throw new Exception("JSON syntax error in response", e); }
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Gets attributes of a set of files/subdirectories
	 * @param uri URI of the remote directory
	 * @param creds Credentials for the remote storage
	 * @param entries
	 * @return JSONArray of entry attributes
	 * @throws Exception On any error
	 */
	public JSONArray attributes(final String uri, final Map<String, String> creds, final String [] entries) throws Exception {
		try {
			JSONArray requestJSON = new JSONArray();
			for (String entry: entries) requestJSON.put(entry);
			
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/attributes");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON);
			addMapAsHttpHeader(b, creds);
			String response = b.post(String.class, requestJSON.toString());
	    	try { return new JSONArray(new JSONTokener(response)); }
	    	catch (JSONException e) { throw new Exception("JSON syntax error in response", e); } 
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}
	
	/**
	 * Creates a Data Avenue HTTP URL (alias) for a file with options
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @param optionsJSON
	 * @return JSONObject of alias details
	 * @throws Exception On any error
	 */
	public JSONObject createAlias(final String uri, final Map<String, String> creds, String optionsJSON) throws Exception {
		if (optionsJSON == null) return createReadAlias(uri, creds);
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/aliases");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON);
			addMapAsHttpHeader(b, creds);
			String response = b.post(String.class, optionsJSON);
	   		try { return new JSONObject(new JSONTokener(response)); }
	   		catch (JSONException e) { throw new Exception("JSON syntax error in response", e); }
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Creates a Data Avenue HTTP URL (alias) for a file with options (parameters)
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @param optionsJSON
	 * @return JSONObject of alias details
	 * @throws Exception On any error
	 */
	public JSONObject createAlias(final String uri, final Map<String, String> creds, final boolean read, final int lifetime, final boolean redirect, final boolean archive) throws Exception {
		
		JSONObject optionsJSON = new JSONObject();
		optionsJSON.put(ALIAS_LIFETIME, lifetime);
		optionsJSON.put(ALIAS_READ, read);
		optionsJSON.put(ALIAS_REDIRECT, redirect);
		optionsJSON.put(ALIAS_ARCHIVE, archive);
		return createAlias(uri, creds, optionsJSON);
	}
	
	/**
	 * Creates a Data Avenue HTTP URL (alias) for a file with options (parameters)
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @param optionsJSON
	 * @return JSONObject of alias details
	 * @throws Exception On any error
	 */
	public JSONObject createAlias(final String uri, final Map<String, String> creds, final boolean read, final int lifetime, final boolean redirect) throws Exception {
		JSONObject optionsJSON = new JSONObject();
		optionsJSON.put(ALIAS_LIFETIME, lifetime);
		optionsJSON.put(ALIAS_READ, read);
		optionsJSON.put(ALIAS_REDIRECT, redirect);
		return createAlias(uri, creds, optionsJSON);
	}
	
	/**
	 * Creates a Data Avenue HTTP URL (alias) for a file with options
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @param optionsJSON Options as JSON object
	 * @return JSONObject of alias details
	 * @throws Exception On any error
	 */
	public JSONObject createAlias(final String uri, final Map<String, String> creds, JSONObject optionsJSON) throws Exception {
		return createAlias(uri, creds, optionsJSON.toString());
	}
	
	/**
	 * Create a Data Avenue HTTP URL (alias) for a file for READING 
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @throws Exception On any error
	 * @return JSONObject of alias details
	 */
	public JSONObject createReadAlias(final String uri, final Map<String, String> creds) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/aliases");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON);
			addMapAsHttpHeader(b, creds);
			String response = b.post(String.class);
	   		try { return new JSONObject(new JSONTokener(response)); }
	   		catch (JSONException e) { throw new Exception("JSON syntax error in response", e); }
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}
	
	/**
	 * Create a Data Avenue HTTP URL (alias) for a file for WRITING 
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @throws Exception On any error
	 * @return JSONObject of alias details
	 */
	public JSONObject createWriteAlias(final String uri, final Map<String, String> creds) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/aliases");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON);
			addMapAsHttpHeader(b, creds);
			String response = b.post(String.class, "{" + ALIAS_READ + ": false}");
	   		try { return new JSONObject(new JSONTokener(response)); }
	   		catch (JSONException e) { throw new Exception("JSON syntax error in response", e); }
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}
	
	/**
	 * Deletes an alias 
	 * @param id ID of the alias
	 * @throws Exception On any error
	 */
	public void deleteAlias(String id) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/aliases" + "/" + id);
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key);
			b.delete();
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Downloads a remote file to the specified local file 
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @param filePath Path of the local file
	 * @throws Exception On any error
	 */	
	public void download(final String uri, final Map<String, String> creds, final String filePath) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/file");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).accept(MediaType.APPLICATION_OCTET_STREAM);
			if (allowRedirects) b.header(HTTP_HEADER_REDIRECT, "yes");
			addMapAsHttpHeader(b, creds);
			InputStream in = b.get(InputStream.class);
			OutputStream fos = new BufferedOutputStream(new FileOutputStream(filePath));
			byte [] buffer = new byte[1024 * 16]; int len = 0;
			while ((len = in.read(buffer)) > 0) fos.write(buffer, 0, len);
			fos.close();
			in.close();
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Gets the input stream of a remote file 
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @return InputStream of the remote file
	 */		
	public InputStream inputStream(final String uri, final Map<String, String> creds) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/file");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).accept(MediaType.APPLICATION_OCTET_STREAM);
			if (allowRedirects) b.header(HTTP_HEADER_REDIRECT, "yes");
			addMapAsHttpHeader(b, creds);
			return b.get(InputStream.class);
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}
	
	/**
	 * Uploads a local file to a remote storage 
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @param filePath Path of the local file
	 * @throws Exception On any error
	 */	
	public void upload(final String uri, final Map<String, String> creds, final String filePath) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/file");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).type(MediaType.APPLICATION_OCTET_STREAM);
			if (allowRedirects) b.header(HTTP_HEADER_REDIRECT, "yes");
			addMapAsHttpHeader(b, creds);
			InputStream in = new FileInputStream(filePath); // BufferedInputStream causes pipe closed exception
			b.post(InputStream.class, in);
			in.close();
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}
	
	/**
	 * Uploads an input stream to a remote storage
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @param in InputStream to be uploaded
	 * @throws Exception On any error
	 */	
	public void upload(final String uri, final Map<String, String> creds, final InputStream in) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/file");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).type(MediaType.APPLICATION_OCTET_STREAM);
			if (allowRedirects) b.header(HTTP_HEADER_REDIRECT, "yes");
			addMapAsHttpHeader(b, creds);
			b.post(InputStream.class, in);
			in.close();
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}
	
	/**
	 * Deletes a remote file
	 * @param uri URI of the remote file
	 * @param creds Credentials for the remote storage
	 * @throws Exception On any error
	 */
	public void delete(final String uri, final Map<String, String> creds) throws Exception {
		try {  
		Client c = getHttpClient();
		WebResource r = c.resource(url + "/file");
		com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri);
		addMapAsHttpHeader(b, creds);
		b.delete();
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Returns the status of the transfer
	 * @param id Transfer if
	 * @return JSON object of transfer details
	 * @throws Exception On any error
	 */
	public JSONObject status(final String id) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/transfers" + "/" + id);
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).accept(MediaType.APPLICATION_JSON);
			String response = b.get(String.class);
	   		try { return new JSONObject(new JSONTokener(response)); }
	   		catch (JSONException e) { throw new Exception("JSON syntax error in response", e); }
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Aborts the transfer
	 * @param id Transfer id
	 * @throws Exception On any error
	 */
	public void cancel(final String id) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/transfers" + "/" + id);
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).accept(MediaType.APPLICATION_JSON);
			b.delete(String.class);
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}	
	
	/**
	 * Starts a new data transfer (copy/move a file or directory from one storage to another)
	 * NOTE: target credentials must have an entry with key Type and value UserPass, Proxy. Username must be stored under key UserID; password under key UserPass. 
	 * @param uri Source URI of a file or directory
	 * @param creds Credentials for the source storage
	 * @param targetDetails Target URI (target), target storage credentials (credentials), and other options as String (in JSON)
	 * @throws Exception On any error
	 * @return Transfer ID as JSONObject
	 */
	public JSONObject transfer(final String uri, final Map<String, String> creds, final String targetDetails) throws Exception {
		return transfer(uri, creds, new JSONObject(targetDetails));
	}

	/**
	 * Starts a new data transfer (copy/move a file or directory from one storage to another)
	 * NOTE: target credentials must have an entry with key Type and value UserPass, Proxy. Username must be stored under key UserID; password under key UserPass. 
	 * @param uri Source URI of a file or directory
	 * @param creds Credentials for the source storage
	 * @param targetDetails Target URI (target), target storage credentials (credentials), and other options in JSON format
	 * @throws Exception On any error
	 * @return Transfer ID as JSONObject
	 */
	public JSONObject transfer(final String uri, final Map<String, String> creds, final JSONObject targetDetails) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/transfers");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).header(HTTP_HEADER_URI, uri).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON);
			addMapAsHttpHeader(b, creds);
			addJasagaKeysToMap(targetDetails);
			String response = b.post(String.class, targetDetails.toString());
	   		try { return new JSONObject(new JSONTokener(response)); }
	   		catch (JSONException e) { throw new Exception("JSON syntax error in response", e); }
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Copies a file or directory from one storage to another
	 * NOTE: target credentials must have an entry with key Type and value UserPass, Proxy. Username must be stored under key UserID; password under key UserPass. 
	 * @param uri Source URI of a file of directory 
	 * @param srcCreds Credentials for the source storage
	 * @param targetUri Target URI of a file of directory
	 * @param targetCreds Credentials for the target storage
	 * @throws Exception On any error
	 * @return Transfer ID as JSONObject
	 */
	public JSONObject copy(final String uri, final Map<String, String> srcCreds, final String targetUri, final Map<String, String> targetCreds) throws Exception {
		JSONObject targetDetails = new JSONObject();
		targetDetails.put(JSON_TARGET, targetUri);
		targetDetails.put(JSON_CREDENTIALS, new JSONObject(targetCreds));
		return transfer(uri, srcCreds, targetDetails.toString());
	}

	/**
	 * Move a file or directory from one storage to another
	 * NOTE: target credentials must have an entry with key Type and value UserPass, Proxy. Username must be stored under key UserID; password under key UserPass. 
	 * @param uri Source URI of a file of directory 
	 * @param srcCreds Credentials for the source storage
	 * @param targetUri Target URI of a file of directory
	 * @param targetCreds Credentials for the target storage
	 * @throws Exception On any error
	 * @return Transfer ID as JSONObject
	 */
	public JSONObject move(final String uri, final Map<String, String> srcCreds, final String targetUri, final Map<String, String> targetCreds) throws Exception {
		JSONObject targetDetails = new JSONObject();
		targetDetails.put(JSON_TARGET, targetUri);
		targetDetails.put(JSON_CREDENTIALS, new JSONObject(targetCreds));
		targetDetails.put(JSON_MOVE, true);
		return transfer(uri, srcCreds, targetDetails.toString());
	}
	
	/**
	 * Returns Data Avenue version
	 * @throws Exception On any error
	 * @return Version as String
	 */
	public String version() throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/version");
			return r.get(String.class);
		} catch (UniformInterfaceException e) {
			e.printStackTrace();
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}
	
	/**
	 * Returns the list of storage protocols supported by Data Avenue
	 * @throws Exception On any error
	 * @return JSONArray of protocols
	 */
	public JSONArray protocols() throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/protocols");
			String response = r.get(String.class);
	   		try { return new JSONArray(new JSONTokener(response)); }
	   		catch (JSONException e) { throw new Exception("JSON syntax error in response", e); }
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Returns operations implemented for the given protocol by Data Avenue 
	 * @param protocol Protocol name
	 * @throws Exception On any error
	 * @return
	 */
	public JSONArray operations(final String protocol) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/operations" + "/" + protocol);
			String response = r.get(String.class);
	   		try { return new JSONArray(new JSONTokener(response)); }
	   		catch (JSONException e) { throw new Exception("JSON syntax error in response", e); }
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}
	
	/**
	 * Returns authentication modes supported for the given protocol
	 * @param protocol Protocol name
	 * @throws Exception On any error
	 * @return JSONArray of authentication modes
	 */
	public JSONArray authentications(final String protocol) throws Exception {
		try {
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/authentication" + "/" + protocol);
			String response = r.get(String.class);
	   		try { return new JSONArray(new JSONTokener(response)); }
	   		catch (JSONException e) { throw new Exception("JSON syntax error in response", e); }
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}

	/**
	 * Creates a new user key
	 * 
	 * @param name Name of the new user
	 * @param company Company name of the new user
	 * @param email E-mail address of the new user
	 * 
	 * @throws Exception On any error
	 * @return String The new key
	 */
	public String createKey(final String name, final String company, final String email) throws Exception {
		try {
			JSONObject requestJSON = new JSONObject();
			if (name != null) requestJSON.put("name",  name);
			if (company != null) requestJSON.put("company",  company);
			if (email != null) requestJSON.put("email",  email);
			
			Client c = getHttpClient();
			WebResource r = c.resource(url + "/keys");
			com.sun.jersey.api.client.WebResource.Builder  b = r.header(HTTP_HEADER_KEY, key).type(MediaType.APPLICATION_JSON);

			return b.post(String.class, requestJSON.toString());
		} catch (UniformInterfaceException e) {
			throw new Exception(e.getMessage() + " (" + e.getResponse().getEntity(String.class) + ")");
		} catch (Throwable e) {
			throw new Exception(e);
		}
	}
	
	private static ClientConfig configureClient() {
		TrustManager[ ] certs = new TrustManager[ ] {
	            new X509TrustManager() {
					@Override
					public X509Certificate[] getAcceptedIssuers() {
						return null;
					}
					@Override
					public void checkServerTrusted(X509Certificate[] chain, String authType)
							throws CertificateException {
					}
					@Override
					public void checkClientTrusted(X509Certificate[] chain, String authType)
							throws CertificateException {
					}
				}
	    };
	    SSLContext ctx = null;
	    try {
	        ctx = SSLContext.getInstance("TLS");
	        ctx.init(null, certs, new SecureRandom());
	    } catch (java.security.GeneralSecurityException ex) {
	    }
	    HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());
	    
	    ClientConfig config = new DefaultClientConfig();
	    try {
		    config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(
		        new HostnameVerifier() {
					@Override
					public boolean verify(String hostname, SSLSession session) {
						return true;
					}
		        }, 
		        ctx
		    ));
	    } catch(Exception e) {
	    }
	    return config;
	}
	
	@SuppressWarnings("unused")
	private static String resolveURL(String dnsServer, String serviceName) throws UnknownHostException {
//		System.out.println("DNS server: " + dnsServer);
//		System.out.println("Service name: " + serviceName);
		System.setProperty("sun.net.spi.nameservice.nameservers", dnsServer);
		System.setProperty("sun.net.spi.nameservice.provider.1", "dns,sun");
		InetAddress[] inetAddressArray = InetAddress.getAllByName(serviceName);
		if (inetAddressArray.length == 0) throw new UnknownHostException("Cannot resolve service name: " + serviceName + " using DNS: " + dnsServer);
		return inetAddressArray[0].getHostAddress();
	}

	private static String resolveURL2(String dnsServer, String serviceName) throws UnknownHostException {
		if (dnsServer == null || serviceName == null) throw new IllegalArgumentException("null parameter");
		// $ dig @<DNS_ADDRESS> -p <PORT_NUMBER> <SERVICE_NAME>
		try {
			Hashtable <String, String> env = new Hashtable<String, String>();
			env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
			String port = dnsServer.contains(":") ? "" : ":53"; // default port: 53
			env.put("java.naming.provider.url",    "dns://" + dnsServer + port); // port number can be changed
			DirContext ictx = new InitialDirContext(env);
			Attributes attrs = ictx.getAttributes(serviceName, new String[] {"A"}); // "MX", "A", "AAAA"
			NamingEnumeration<? extends Attribute> attrsEnum = attrs.getAll();

			if (attrsEnum.hasMoreElements()) {
				Attribute attr = attrsEnum.next();
				NamingEnumeration<?> attrVals = attr.getAll();
				if (attrVals.hasMoreElements()) {
					Object o = attrVals.next();
					if (o instanceof String) return (String) o; // return the first one
				}
			} 
			
			throw new UnknownHostException("Cannot resolve service name: " + serviceName + " using DNS: " + dnsServer);
		} catch (Exception e) { throw new UnknownHostException(e.getMessage()); }
	}
}	