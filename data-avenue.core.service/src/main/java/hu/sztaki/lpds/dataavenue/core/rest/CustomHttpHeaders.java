package hu.sztaki.lpds.dataavenue.core.rest;

public class CustomHttpHeaders {
	// general
	static final String HTTP_HEADER_KEY = "x-key";
	static final String HTTP_HEADER_URI = "x-uri";
	
	// credential attributes
	static final String HTTP_HEADER_USERNAME = "x-username";
	static final String HTTP_HEADER_PASSWORD = "x-Password";
	static final String HTTP_HEADER_PROXY = "x-proxy";
	static final String HTTP_HEADER_PROXY_TYPE = "x-credential-proxy-type"; // e.g., X-Proxy-Type: VOMS
	static final String HTTP_HEADER_VO = "x-credential-vo"; 
	static final String HTTP_HEADER_CERTIFICATE = "x-credential-certificate";
	static final String HTTP_HEADER_RESOURCE = "x-credential-resource"; // iRODS resource
	static final String HTTP_HEADER_ZONE = "x-credential-zone"; // iRODS source
	
	// redirects, sessions
	static final String HTTP_HEADER_REDIRECT = "x-accept-redirects"; // yes | no
	static final String HTTP_HEADER_USE_SESSION = "x-use-session"; // yes (any value) | no (if header absent)

	static final String HTTP_HEADER_CREDENTIALS = "x-credentials";
	static final String HTTP_HEADER_DETAILS = "x-details";
}