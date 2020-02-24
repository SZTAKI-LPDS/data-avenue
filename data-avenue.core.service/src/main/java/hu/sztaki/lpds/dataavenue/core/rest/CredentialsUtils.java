package hu.sztaki.lpds.dataavenue.core.rest;

import java.util.Iterator;
import java.util.Set;

import hu.sztaki.lpds.dataavenue.core.interfaces.impl.CredentialsImpl;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import static hu.sztaki.lpds.dataavenue.core.rest.CustomHttpHeaders.*;
import static hu.sztaki.lpds.dataavenue.interfaces.CredentialsConstants.*;

import javax.ws.rs.core.HttpHeaders;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CredentialsUtils {
	private static final Logger log = LoggerFactory.getLogger(CredentialsUtils.class); 

	@SuppressWarnings("unchecked")
	static Credentials createCredentialsFromHttpHeader(final HttpHeaders httpHeaders) {
		
		Credentials credentials = new CredentialsImpl();

		// process x-credentials header
		if (httpHeaders.getRequestHeader(HTTP_HEADER_CREDENTIALS) != null) {
			String xCredentialsHeader = httpHeaders.getRequestHeader(HTTP_HEADER_CREDENTIALS).get(0);
			// parse json
			JSONObject credentialsFieldsJSON = null;
			try { 
				JSONTokener tokener = new JSONTokener(xCredentialsHeader);
				credentialsFieldsJSON = new JSONObject(tokener);
				for (String key: (Set<String>) credentialsFieldsJSON.keySet()) credentials.putCredentialAttribute(key, credentialsFieldsJSON.optString(key));
				// workaround for backward compatibility, add "Type" (upper initial) as "type"
				if (credentials.getCredentialAttribute(TYPE) == null && credentials.getCredentialAttribute(TYPE_UPPER_INITIAL) != null)
					credentials.putCredentialAttribute(TYPE, credentials.getCredentialAttribute(TYPE_UPPER_INITIAL));

				credentialsFieldsJSON = null;
				tokener = null;
			}
    		catch (JSONException e) { 
    			log.error("Header " + HTTP_HEADER_CREDENTIALS + " contains invalid JSON: " + e.getMessage());
    			return null;
    		}
			
		} else {
			// create credential attributes from (raw) header values
			mapHeaderToAttribute(HTTP_HEADER_USERNAME, USERID, httpHeaders, credentials);
			mapHeaderToAttribute(HTTP_HEADER_PASSWORD, USERPASS, httpHeaders, credentials);
			
			// create credential attributes from base64 encoded header values
			mapHeaderToAttributeBase64(HTTP_HEADER_PROXY, USERPROXY, httpHeaders, credentials);
			
			// predict credential type
			if (credentials.getCredentialAttribute(USERPASS) != null) { // UserPass
				credentials.putCredentialAttribute(TYPE, TYPE_USERPASS);
			} else if (credentials.getCredentialAttribute(USERPROXY) != null) { // X509
				String proxyType = httpHeaders.getRequestHeader(HTTP_HEADER_PROXY_TYPE) != null ? httpHeaders.getRequestHeader(HTTP_HEADER_PROXY_TYPE).remove(0) : null; 
				if (proxyType != null) credentials.putCredentialAttribute(TYPE, proxyType); //  X-Proxy-Type: "VOMS"
				else credentials.putCredentialAttribute(TYPE, TYPE_GLOBUS); // "Globus" by default
			}
			
			// process rest of the headers
			processRemainingHeaderAttributes(httpHeaders, credentials);

			// add lower case "type" if missing
			if (credentials.getCredentialAttribute(TYPE) == null && credentials.getCredentialAttribute(TYPE_UPPER_INITIAL) != null)
				credentials.putCredentialAttribute(TYPE, credentials.getCredentialAttribute(TYPE_UPPER_INITIAL));
		}
		return credentials.keySet().size() > 0 ? credentials : null;
	}
	
	private static void mapHeaderToAttribute(final String header, final String attribute, final HttpHeaders httpHeaders, final Credentials credentials) {
		if (httpHeaders.getRequestHeader(header) == null) return;
		credentials.putCredentialAttribute(attribute, httpHeaders.getRequestHeader(header).get(0));
	}
	
	private static void mapHeaderToAttributeBase64(final String header, final String attribute, final HttpHeaders httpHeaders, final Credentials credentials) {
		if (httpHeaders.getRequestHeader(header) == null) return;
		String headerValue = httpHeaders.getRequestHeader(header).get(0);
		if (headerValue.startsWith("Base64 ")) headerValue = headerValue.substring(7);
		byte [] bytes = Base64.decodeBase64(headerValue.getBytes());
		credentials.putCredentialAttribute(attribute, new String(bytes));
	}

	// map header "X-Credential-Name: value" to attribute Name->value
	// header values starting with "Base64 " will be decoded 
	private static void processRemainingHeaderAttributes(final HttpHeaders httpHeaders, final Credentials credentials) {
		for (String headerName: httpHeaders.getRequestHeaders().keySet()) {
			for (String headerValue: httpHeaders.getRequestHeader(headerName)) {
				if (!headerValue.startsWith("X-Credential-")) continue;
				String credentialName = headerName.substring(13);
				if (!headerValue.startsWith("Base64 ")) { // plain text
					credentials.putCredentialAttribute(credentialName, headerValue);
				} else { // base64 encoded value
					byte [] bytes = Base64.decodeBase64(headerValue.substring(7).getBytes());
					credentials.putCredentialAttribute(credentialName, new String(bytes));
				}
			}
		}
	}
	
	public static Credentials createCredentialsFromJSON(final JSONObject credentialsJSON) {
		Credentials credentials = new CredentialsImpl();
		for (@SuppressWarnings("unchecked") Iterator<String> i = credentialsJSON.keys(); i.hasNext(); ) {
			String key = (String) i.next();
			String value;
			try { value = credentialsJSON.get(key).toString(); } 
			catch (JSONException e) 
			{
				log.error("Invalid content for key: " + key);
				continue; 
			}
			
			credentials.putCredentialAttribute(key, value);
		}

		// add lower case "type" if missing
		if (credentials.getCredentialAttribute(TYPE) == null && credentials.getCredentialAttribute(TYPE_UPPER_INITIAL) != null)
			credentials.putCredentialAttribute(TYPE, credentials.getCredentialAttribute(TYPE_UPPER_INITIAL));

		return credentials;
	}
}