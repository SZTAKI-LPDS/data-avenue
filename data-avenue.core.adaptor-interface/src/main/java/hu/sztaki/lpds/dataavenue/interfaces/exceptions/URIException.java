package hu.sztaki.lpds.dataavenue.interfaces.exceptions;

/**
 * @author Akos Hajnal
 */
@SuppressWarnings("serial")
public class URIException extends Exception {
	public URIException(String msg) { super(msg);	}
	
	public URIException(Exception x) {
		this("Malformed URI!", x);
	}
	
	public URIException(String msg, Exception x) {
		super(msg + ExceptionUtils.getExceptionTrace(x)); 
	}
}
