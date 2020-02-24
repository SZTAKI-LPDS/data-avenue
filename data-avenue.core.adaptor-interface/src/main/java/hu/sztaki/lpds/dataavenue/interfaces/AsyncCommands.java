package hu.sztaki.lpds.dataavenue.interfaces;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

/**
 * @author Akos Hajnal
 * 
 * Interface collecting asynchronous commands that an adaptor must/may implement.
 * 
 * These calls are non-blocking, i.e. they may return before the command is fully completed, and return an id
 * can used to query the state of command execution or cancel the task.
 *
 */
public interface AsyncCommands {
	/**
	 * Copies the specified file to a new location when protocols of both source and destination locations are handled by the same adaptor.
	 * The operation is performed asynchronously; the call likely returns before having the copy operation fully completed.  
	 * The returned id is for querying/cancelling the transfer.
	 * 
	 * Implementors may choose not to implement this method (and throw OperationException), in which case, DataAvenue will perform copy
	 * reading and writing from streams. Adaptors should implement this method, if they can perform copy more efficiently than low level 
	 * streaming - e.g., by using of third-party transfer. 
	 *  
	 * @param fromUri Location of the source file
	 * @param fromCredentials Credentials required to access the source file
	 * @param toUri Location of the file to be copied
	 * @param toCredentials Credentials required to access the target resource
 	 * @param overwrite Overwrite target file if exists
	 * @param monitor Monitor to maintain task status
     * @return Internal id of the adaptor managed task
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
	 */
	public String copy(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException;

	/**
	 * Moves the specified file to a new location when protocols of both source and destination locations are handled by the same adaptor.
	 * The operation is performed asynchronously; the call likely returns before having the move operation fully completed.  
	 * The returned id is for querying/cancelling the transfer.
	 * 
	 * Implementors may choose not to implement this method (and throw OperationException), in which case DataAvenue will perform move operation
	 * reading and writing from streams. Adaptors should implement this method, if they can realize move operation more efficiently than low level 
	 * streaming - e.g., by using of third-party transfer. 
	 *  
	 * @param fromUri Location of the source file
	 * @param fromCredentials Credentials required to access the source file
	 * @param toUri Location of the file to be copied
	 * @param toCredentials Credentials required to access the target resource
	 * @param overwrite Overwrite target file if exists
	 * @param monitor Monitor to maintain task status
     * @return Internal id of the task by which its progress can be queried
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
	 */
	public String move(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException;
	
	/**
	 * Cancels a task.
	 * 
	 * @param id Internal id of the adaptor managed task
     * @throws TaskIdException If the task id is invalid
     * @throws OperationException If cannot cancel the task
	 */
	public void cancel(String id) throws TaskIdException, OperationException;
}
