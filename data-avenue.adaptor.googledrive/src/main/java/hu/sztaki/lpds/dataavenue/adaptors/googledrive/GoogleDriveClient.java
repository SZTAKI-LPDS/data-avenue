package hu.sztaki.lpds.dataavenue.adaptors.googledrive;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;

import hu.sztaki.lpds.dataavenue.interfaces.URIBase;

public class GoogleDriveClient {

private final static Map<String, Drive> clients = new HashMap<String, Drive>(); // map: host -> client
	
	// Get the client from the dropbox, authentication process
	GoogleDriveClient withClient(final URIBase uri, final String access) throws IOException, GeneralSecurityException {
       GoogleCredential credential = new GoogleCredential();
          credential.setAccessToken(access);
  		  String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
          Drive service = new Drive.Builder(GoogleNetHttpTransport.newTrustedTransport(), JacksonFactory.getDefaultInstance(), credential)
                  .setApplicationName("asd")
                  .build();
		clients.put(hostAndPort,service);
		return this;
	}
	
	// Get back the client with the uri
	Drive get(final URIBase uri) {
		String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
		return clients.get(hostAndPort);
	}
}
