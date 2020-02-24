package hu.sztaki.lpds.dataavenue.interfaces.exceptions;

@SuppressWarnings("serial")
public class CredentialException extends Exception {

	public CredentialException(String msg) { super(msg);	}
	
	public CredentialException(Exception x) {
		this("Invalid or missing credentials!", x);
	}
	
	public CredentialException(String msg, Exception x) {
		super(msg + ExceptionUtils.getExceptionTrace(x)); 
	}
}
