package hu.sztaki.lpds.dataavenue.interfaces.exceptions;

@SuppressWarnings("serial")
public class OperationNotSupportedException extends Exception {
	public OperationNotSupportedException() { super("Operation not supported"); }
	public OperationNotSupportedException(String msg) { super(msg);	}
}
