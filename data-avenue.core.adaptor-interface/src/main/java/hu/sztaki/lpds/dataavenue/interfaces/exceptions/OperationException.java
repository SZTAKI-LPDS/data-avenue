/**
 * 
 */
package hu.sztaki.lpds.dataavenue.interfaces.exceptions;

/**
 * @author Akos Hajnal
 */
@SuppressWarnings("serial")
public class OperationException extends Exception {

	public OperationException(String msg) { super(msg);	}
	
	public OperationException(Throwable x) {
		this(x.getMessage());
	}
	
	public OperationException(String msg, Throwable x) {
		super(msg + ExceptionUtils.getExceptionTrace(x)); 
	}
}