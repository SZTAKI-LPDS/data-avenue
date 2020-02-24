package hu.sztaki.lpds.dataavenue.interfaces.exceptions;

@SuppressWarnings("serial")
/*
 * Thrown by AsyncCommands.cancel(), if invalid task id provided.
 */
public class TaskIdException extends Exception {

	public TaskIdException(String msg) {
		super(msg);
	}
	
	public TaskIdException(Exception x) {
		this("Invalid task id!", x);
	}
	
	public TaskIdException(String msg, Exception x) {
		super(msg + ExceptionUtils.getExceptionTrace(x)); 
	}
}