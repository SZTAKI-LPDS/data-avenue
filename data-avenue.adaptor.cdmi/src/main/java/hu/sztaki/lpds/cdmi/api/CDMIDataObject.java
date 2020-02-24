package hu.sztaki.lpds.cdmi.api;

import static hu.sztaki.lpds.cdmi.api.CDMIConstants.*;
import static hu.sztaki.lpds.cdmi.api.HTTPConstants.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.json.simple.parser.ParseException;

public class CDMIDataObject {
	
	// @link: http://snia.org/sites/default/files/CDMI%20v1.0.2.pdf p.53.
	public static Map<String, Object> getCDMIMetaData(String uri) throws CDMIURIException, CDMIOperationException {
		if (uri == null) throw new CDMIURIException("No URI provided!");
		if (uri.endsWith(CDMIConstants.PATH_SEPARATOR)) throw new CDMIURIException("URI is a container object! (" + uri + ")");

		// TODO check required capabilities: cdmi_read_metadata
		
		// create HTTP GET to retrieve children names
		HttpGet httpGet = new HttpGet(uri + "?" + CDMI_METADATA); // ? query children attribute only
		//HttpGet httpGet = new HttpGet(uri + "?" + CDMI_METADATA:<prefix>); // size?
		httpGet.setHeader("Accept", CDMI_OBJECT); // optional
		httpGet.setHeader("X-CDMI-Specification-Version", CDMI_SPECIFICATION_VERSION); // mandatory

		// get HTTP client
		HttpClient httpClient = CDMIHTTPClient.getClient();
		
		try {
			HttpResponse response = null;
			try {
				// send request
				response = httpClient.execute(httpGet); 
			} catch (Exception e) { // ClientProtocolException, IOException
				throw new CDMIOperationException(e);
			}
	
			int status = response.getStatusLine().getStatusCode();
			String statusMsg = response.getStatusLine().toString();
			
			// check status code
			switch (status) {
				case REQUEST_OK: // The metadata for the container object is provided in the message body
					break;
				case REQUEST_ACCEPTED: // The data object is in the process of being created. 
									   // The CDMI client should monitor the completionStatus and percentComplete fields to determine the current status of the operation.
					throw new CDMIOperationException("Data object is in the process of being created.!");
				case REQUEST_FOUND: // The URI is a reference to another URI.
					throw new CDMIOperationException("References are not supported!");
					// TODO Header String "Location" The server shall respond with the URI that the reference redirects to if the object is a reference.
				case REQUEST_BAD: // The request contains invalid parameters or field names.
					throw new CDMIOperationException(statusMsg);
				case REQUEST_UNAUTHORIZED: // The authentication credentials are missing or invalid.
					throw new CDMIOperationException("Authentication credentials are missing or invalid!");
				case REQUEST_FORBIDDEN: // The client lacks the proper authorization to perform this request.
					throw new CDMIOperationException("Authorization failed!");
				case REQUEST_NOT_FOUND: // The resource was not found at the specified URI.
					throw new CDMIURIException("Object does not exist! (" + uri + ")");
				case REQUEST_NOT_ACCEPTABLE: // The server is unable to provide the object in the content type specified in the	Accept header.
					throw new CDMIOperationException("Server is unable to provide the expected content type!");
				default: 
					throw new CDMIOperationException("Unexpected response status code! (" + status + ")");
			}
			
			response.getHeaders(HTTP_HEADER_CONTENT_LENGTH); // file size if get non-CDMI content
			
			HttpEntity entity = null;
			try {
				entity = response.getEntity();
				if (entity == null) throw new CDMIOperationException("Invalid server response! (No HTTP body.)");  

				InputStreamReader entityStream = null;
				try {
					try { 
						// read HTTP body
						entityStream = new InputStreamReader(entity.getContent()); 
					} catch (Exception e) { // IllegalStateException, IOException 
						throw new CDMIOperationException(e); 
					} 
					
					try { 
						Map<String, Object> metadataMap = CDMIJason.getMetadataMap(entityStream); 
						return metadataMap;
					} catch (ParseException e) { 
						throw new CDMIOperationException("Invalid server response! (Invalid JSON content or no children attribute provided.)"); 
					} 
				
				} finally { 
					if (entityStream != null) try { entityStream.close(); } catch(IOException e) {} 
				}
				
			} finally {
				if (entity != null) { try { EntityUtils.consume(entity); } catch (IOException e) {} }
			}
		
		} finally { 
			httpGet.releaseConnection();
		}
	}

	
	public static void main(String [] args) throws Exception {
		System.out.println(CDMIDataObject.getCDMIMetaData("http://localhost:8082/cdmi-server/TestContainer/testfile2.txt"));
		
	}
}
