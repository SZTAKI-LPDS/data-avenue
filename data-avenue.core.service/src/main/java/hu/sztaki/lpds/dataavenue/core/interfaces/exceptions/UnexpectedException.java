package hu.sztaki.lpds.dataavenue.core.interfaces.exceptions;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;

@SuppressWarnings("serial")
public class UnexpectedException extends OperationException {

	public UnexpectedException() {
		super("Internal server error!");
	}
	public UnexpectedException(Throwable x) {
		super("Internal server error!", x);
	}
}