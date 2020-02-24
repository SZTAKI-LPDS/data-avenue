package hu.sztaki.lpds.dataavenue.adaptors.s3;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import hu.sztaki.lpds.dataavenue.interfaces.CloseableSessionObject;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;

class S3Clients implements CloseableSessionObject {
	
	private static final Logger log = LoggerFactory.getLogger(S3Clients.class);
	
	static int MAX_CONNECTIONS = 5; // number of ports opened by one client
	static boolean DISABLE_HOSTNAME_VERIFICATION = false; // disable hostname verification
	
	private final Map<String, AmazonS3Client> clients = new HashMap<String, AmazonS3Client>(); // map: host -> client
	
	S3Clients withClient(final URIBase uri, final String accessKey, final String secretKey) {
		add(uri, accessKey, secretKey);
		return this;
	}
	
	void add(final URIBase uri, final String accessKey, final String secretKey) {
		add(uri.getHost(), uri.getPort(), accessKey, secretKey);
	}

	private void add(final String host, Integer port, final String accessKey, final String secretKey) {
		
		AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey); // create credentials

		ClientConfiguration clientConfiguration = new ClientConfiguration(); // protocol, max connections
		
		clientConfiguration.setProtocol(Protocol.HTTPS); // use https connection
		// unless port 80 is explicitely added
		if (port != null && port == 80) clientConfiguration.setProtocol(Protocol.HTTP); // use http connection on port 80

		if (DISABLE_HOSTNAME_VERIFICATION) disableCertificateVerification(clientConfiguration);
		
		clientConfiguration.setMaxConnections(MAX_CONNECTIONS); // set max connections (ports opened by client)
		clientConfiguration.setMaxErrorRetry(PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY * 2); // DEFAULT_MAX_ERROR_RETRY = 3
		clientConfiguration.setConnectionTimeout(ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT * 2); // DEFAULT_CONNECTION_TIMEOUT = 50 * 1000
		clientConfiguration.setSignerOverride("S3SignerType");
		AmazonS3Client amazonS3Client = new AmazonS3Client(awsCredentials, clientConfiguration);

		@SuppressWarnings("deprecation")
		S3ClientOptions clientOptions = new S3ClientOptions().withPathStyleAccess(true); // don't use http://bucket.endpoint style access but http://endpoint/bucket (<= site cert problems)

		amazonS3Client.setS3ClientOptions(clientOptions); // path style access

		String hostAndPort = host + (port != null ? ":" + port : "");
		amazonS3Client.setEndpoint(hostAndPort); // set endpoint
		
		clients.put(hostAndPort, amazonS3Client);
	}

	AmazonS3Client get(final URIBase uri) {
		String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
		return clients.get(hostAndPort);
	}
	
	@Override public void close() {
		for (AmazonS3Client client: clients.values()) {
			try { client.shutdown(); } 
			catch (Exception e) { log.warn("Cannot shutdown client", e); }
		}
	}
	
	// trust all server/client certificates
	private final static TrustManager[] trustAllCerts = new TrustManager[] {
		new X509TrustManager() {
			@Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
			@Override public void checkClientTrusted(X509Certificate[] chain, String authType) {}
			@Override public void checkServerTrusted(X509Certificate[] chain, String authType) {}
		}
	};
	// create ssl connection socket factory that trusts all server/client certificates
	@SuppressWarnings("deprecation")
	private static SSLConnectionSocketFactory getSslSocketFactory() {
		try {
			SSLContext sslContext = SSLContext.getInstance("SSL");
			sslContext.init(null, trustAllCerts, new SecureRandom());
			return new SSLConnectionSocketFactory(sslContext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
		} 
		catch (NoSuchAlgorithmException x) { log.error("Cannot create SSLConnectionSocketFactory", x); }
		catch (KeyManagementException x) { log.error("Cannot create SSLConnectionSocketFactory", x); }
		return null;
	}
	// set ssl connection socket factory in client that trusts all server/client certificates; tested on: apache http Client 4.5
	static void disableCertificateVerification(ClientConfiguration clientConfiguration) {
		SSLConnectionSocketFactory sslSocketFactory = getSslSocketFactory();
		if (sslSocketFactory != null) clientConfiguration.getApacheHttpClientConfig().setSslSocketFactory(sslSocketFactory);
	}
}