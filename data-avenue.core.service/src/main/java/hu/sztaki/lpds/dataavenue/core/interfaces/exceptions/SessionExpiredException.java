/**
 * Thrown when session expired (authentication data resubmit required).
 */
package hu.sztaki.lpds.dataavenue.core.interfaces.exceptions;

/**
 * @author Akos Hajnal
 */
@SuppressWarnings("serial")
public class SessionExpiredException extends Exception {

	public SessionExpiredException() { super("Session expired!"); }
	
}