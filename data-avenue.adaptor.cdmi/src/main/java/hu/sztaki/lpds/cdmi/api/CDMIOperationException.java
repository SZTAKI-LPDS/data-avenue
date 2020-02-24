package hu.sztaki.lpds.cdmi.api;

@SuppressWarnings("serial")
public class CDMIOperationException extends Exception {
	public CDMIOperationException(String msg) {
		super(msg);
	}
	public CDMIOperationException(Exception x) {
		super(x);
	}
}
