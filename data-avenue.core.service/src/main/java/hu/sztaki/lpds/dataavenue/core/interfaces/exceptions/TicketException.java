package hu.sztaki.lpds.dataavenue.core.interfaces.exceptions;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.ExceptionUtils;

@SuppressWarnings("serial")
public class TicketException extends Exception {

	public TicketException(String msg) {
		super(msg);
	}

	public TicketException(Exception x) {
		this("Invalid/expired ticket!", x);
	}
	
	public TicketException(String msg, Exception x) {
		super(msg + ExceptionUtils.getExceptionTrace(x)); 
	}
}