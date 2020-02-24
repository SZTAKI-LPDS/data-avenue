/**
 * 
 */
package hu.sztaki.lpds.dataavenue.core.interfaces.exceptions;

/**
 * @author Akos Hajnal
 *
 */
@SuppressWarnings("serial")
public class NotSupportedProtocolException extends Exception {

	public NotSupportedProtocolException(String msg) {
		super(msg);
	}

	public NotSupportedProtocolException(Exception x) {
		this("Protocol not supported!", x);
	}

	public NotSupportedProtocolException(String msg, Exception x) {
		super(msg, x);
	}

}
