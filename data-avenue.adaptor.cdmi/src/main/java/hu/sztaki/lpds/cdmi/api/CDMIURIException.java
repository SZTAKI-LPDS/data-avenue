package hu.sztaki.lpds.cdmi.api;

@SuppressWarnings("serial")
public class CDMIURIException extends Exception {
	public CDMIURIException(String msg) {
		super("Malformed URL: " + msg);
	}

	public CDMIURIException(Exception x) {
		super("Malformed URL!", x);
	}
}
