package hu.sztaki.lpds.dataavenue.interfaces;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

/*
 * Adaptor may support creating direct URLs for resources (e.g., pre-signed URLs in the case of S3) and the adaptor must indicate it
 * towards DataAvenue framework. If so, at downloads DataAvenue will choose the direct way.
 */
public interface DirectURLsSupported {
	 /**
     * Creates direct URL for a resource.
     * 
     * @param uri Location of the file to be removed
     * @param credentials Credentials required to acces the resource
     * @param read Direct URL for reading (true) or writing (false)
     * @param lifetime Lifetime of the URL created (in seconds)
     * 
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public String createDirectURL(URIBase uri, Credentials credentials, boolean read, int lifetime, DataAvenueSession session) throws URIException, OperationException, CredentialException;  
}
