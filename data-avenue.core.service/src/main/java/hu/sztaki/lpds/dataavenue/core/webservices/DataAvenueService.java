package hu.sztaki.lpds.dataavenue.core.webservices;

import hu.sztaki.lpds.dataavenue.core.TransferDetails;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.SessionExpiredException;
import hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.TicketException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

import java.io.IOException;
import java.util.List;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;

@Deprecated
@WebService(
		name="DataAvenueService", // sets <portType name="DataAvenueService">
		targetNamespace="http://ws.dataavenue.lpds.sztaki.hu/"
		//serviceName="", // modeler error: The @javax.jws.WebService.serviceName element cannot be specified on a service endpoint interface.
		//portName="" // modeler error: The @javax.jws.WebService.portName element cannot be specified on a service endpoint interface. 
		)
public interface DataAvenueService {
	
    /**
     * Returns the list of resource protocols that DataAvenue supports.
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @return List of protocol names (gsiftp, sftp, etc.)
     * @throws CredentialException If the ticket is invalid/expired
     * @throws TicketException If the provided ticket is invalid/expired 
     * @throws SessionExpiredException If session expired
     * @throws OperationException Unexpected exception
     */
	@WebMethod public List<String> getSupportedProtocols(@WebParam(name="ticket") String ticket) throws TicketException, CredentialException, SessionExpiredException, OperationException;
    
    /**
     * Returns the list of operations supported by DataAvenue on the given protocol.
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param protocol Name of the protocol (e.g., gsiftp)
     * @return List of operation names (list, mkdir, delete, rename, copy, etc.)
     * @throws CredentialException If the ticket is invalid/expired
     * @throws NotSupportedProtocolException Protocol is not supported by DataAvenue
     * @throws SessionExpiredException If session expired
     * @throws OperationException Unexpected exception
     */
	@WebMethod public List<OperationsEnum> getSupportedOperations(@WebParam(name="ticket") String ticket, @WebParam(name="protocol") String protocol) throws TicketException, NotSupportedProtocolException, CredentialException, SessionExpiredException, OperationException;
    
    /**
     * Returns the list of possible authentication types required to access resources using the given protocol.
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param protocol Name of the protocol to be used (e.g., gsiftp)
     * 
     * @return List of authentication types (None, Globus, UserPass, MyProxy, VOMS, etc.)
     * 
     * @throws TicketException If the provided ticket is invalid/expired 
     * @throws NotSupportedProtocolException If the protocol is not supported by DataAvenue
     * @throws CredentialException If the ticket is invalid/expired
     * @throws SessionExpiredException If session expired
     * @throws OperationException Unexpected exception
     */
	@WebMethod public List<String> getSupportedAuthenticationTypes(@WebParam(name="ticket") String ticket, @WebParam(name="protocol") String protocol) throws TicketException, NotSupportedProtocolException, CredentialException, SessionExpiredException, OperationException;
    
    /**
     * Returns the list of directory contents (file and subdirectory entries). 
     * Details about files and subdirectories such as last modification date, size are also provided.
     * (Directory name may omit terminating '/'.)
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param uri Directory location (URL)
     * @param credentials Credentials required to acces the resource
     * 
     * @return List of directory entires (files and subdirectories)
     * 
     * @throws TicketException If the provided ticket is invalid/expired 
     * @throws NotSupportedProtocolException If the protocol is not supported by DataAvenue
     * @throws URIException If the given uri is invalid (inaccessible, of invalid format, not a directory)
     * @throws CredentialException If credentials are invalid or the ticket is invalid/expired
     * @throws OperationException If the given operation cannot be performed
     * @throws SessionExpiredException If the session expired (authentication data resubmit required)
     */
	@WebMethod public List<DirEntry> list(@WebParam(name="ticket") String ticket, @WebParam(name="uri") String uri, @WebParam(name="credentials") CredentialAttributes creds) throws TicketException, URIException, NotSupportedProtocolException, OperationException, CredentialException, SessionExpiredException;
    
    /**
     * Creates a new subdirectory. (Directory name may omit terminating '/'.)
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param uri Location of the parent directory where new directory is to be create
     * @param credentials Credentials required to acces the resource
     * 
     * @throws TicketException If the provided ticket is invalid/expired 
     * @throws NotSupportedProtocolException If the protocol is not supported by DataAvenue
     * @throws URIException If the given uri or newDirName is invalid (inaccessible, of invalid format, not a directory)
     * @throws CredentialException If credentials are invalid or the ticket is invalid/expired
     * @throws OperationException If the given operation cannot be performed (e.g., the subdirectory already exists)
     * @throws SessionExpiredException If the session expired (authentication data resubmit required)
     */
	@WebMethod public void mkdir(@WebParam(name="ticket") String ticket, @WebParam(name="uri") String uri, @WebParam(name="credentials") CredentialAttributes creds) throws TicketException, URIException, NotSupportedProtocolException, OperationException, CredentialException, SessionExpiredException;

    /**
     * Removes a directory (with all its subdirectories and files recursively). (Directory name may omit terminating '/'.) 
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param uri Location of the directory to be removed
     * @param credentials Credentials required to acces the resource
     * 
     * @throws TicketException If the provided ticket is invalid/expired 
     * @throws NotSupportedProtocolException If the protocol is not supported by DataAvenue
     * @throws URIException If the given uri is invalid (inaccessible, of invalid format, not a directory)
     * @throws CredentialException If credentials are invalid or the ticket is invalid/expired
     * @throws OperationException If the given operation cannot be performed
     * @throws SessionExpiredException If the session expired (authentication data resubmit required)
     */
	@WebMethod public void rmdir(@WebParam(name="ticket") String ticket, @WebParam(name="uri") String uri, @WebParam(name="credentials") CredentialAttributes creds) throws TicketException, URIException, NotSupportedProtocolException, OperationException, CredentialException, SessionExpiredException;
    
    /**
     * Deletes the specified file.
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param uri Location of the file to be removed
     * @param credentials Credentials required to acces the resource
     * 
     * @throws TicketException If the provided ticket is invalid/expired 
     * @throws NotSupportedProtocolException If the protocol is not supported by DataAvenue
     * @throws URIException If the given uri is invalid (inaccessible, of invalid format)
     * @throws CredentialException If credentials are invalid or the ticket is invalid/expired
     * @throws OperationException If the given operation cannot be performed
     * @throws SessionExpiredException If the session expired (authentication data resubmit required)
     */
	@WebMethod public void delete(@WebParam(name="ticket") String ticket, @WebParam(name="uri") String uri, @WebParam(name="credentials") CredentialAttributes creds) throws TicketException, URIException, NotSupportedProtocolException, OperationException, CredentialException, SessionExpiredException;
	
	 /**
     * Sets permissions of the specified file.
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param uri Location of the file to be removed
     * @param permissions Permissions string
     * @param credentials Credentials required to acces the resource
     * 
     * @throws TicketException If the provided ticket is invalid/expired 
     * @throws NotSupportedProtocolException If the protocol is not supported by DataAvenue
     * @throws URIException If the given uri is invalid (inaccessible, of invalid format)
     * @throws CredentialException If credentials are invalid or the ticket is invalid/expired
     * @throws OperationException If the given operation cannot be performed
     * @throws SessionExpiredException If the session expired (authentication data resubmit required)
     */
	@WebMethod public void setPermissions(@WebParam(name="ticket") String ticket, @WebParam(name="uri") String uriString, @WebParam(name="permissions") String permissionsString, @WebParam(name="credentials") CredentialAttributes creds) throws TicketException, URIException, NotSupportedProtocolException, OperationException, CredentialException, SessionExpiredException;
	
    
    /**
     * Renames a file or a directory. 
     * NOTE: if a directory is to be renamed uri must end with '/', otherwise no terminating slash is allowed. 
     * Parameter 'newName' may or may not end with '/' even on directory renaming.
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param uri Location of the file or directory to be renamed
     * @param newName New name of the file or directory
     * @param credentials Credentials required to acces the resource
     * 
     * @throws TicketException If the provided ticket is invalid/expired 
     * @throws NotSupportedProtocolException If the protocol is not supported by DataAvenue
     * @throws URIException If the given uri is invalid (inaccessible, of invalid format)
     * @throws CredentialException If credentials are invalid or the ticket is invalid/expired
     * @throws OperationException If the given operation cannot be performed (e.g., target file name alredy exists)
     * @throws SessionExpiredException If the session expired (authentication data resubmit required)
     */
	@WebMethod public void rename(@WebParam(name="ticket") String ticket, @WebParam(name="uri") String uri, @WebParam(name="newName") String newName, @WebParam(name="credentials") CredentialAttributes creds) throws TicketException, URIException, NotSupportedProtocolException, OperationException, CredentialException, SessionExpiredException;
    
    /**
     * Copies a file to the specified location in asynchronous way - getProgress web service operation call returns percentage completed information.
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param uri Location of the file to be copied
     * @param credentials Credentials data required to acces the source resource (file to be copied)
     * @param targetUri Location of the target directory to where the file is to be copied
     * @param targetCredentials Credentials data required to acces the destination resource
     * @param overwrite Overwrite target file if already exists
     * 
     * @return UUID DataAvenue assigned task id
     * 
     * @throws TicketException If the provided ticket is invalid/expired 
     * @throws NotSupportedProtocolException If the protocol is not supported by DataAvenue 
     * @throws URIException The given uri or targetUri is invalid (inaccessible, of invalid format)
     * @throws CredentialException If credentials are invalid or the ticket is invalid/expired
     * @throws OperationException If the given operation cannot be performed (e.g., the file already exists on target resource)
     * @throws SessionExpiredException If the session expired (authentication data resubmit required)
     */
	@WebMethod public String copy(@WebParam(name="ticket") String ticket, @WebParam(name="uri") String uri, @WebParam(name="credentials") CredentialAttributes creds, @WebParam(name="targetUri") String targetUri, @WebParam(name="targetCredentials") CredentialAttributes targetCreds, @WebParam(name="overwrite") boolean overwrite) throws TicketException, URIException, OperationException, CredentialException, NotSupportedProtocolException, IOException, SessionExpiredException;

	/**
     * Moves a file to the specified location in asynchronous way - getProgress web service operation call returns percentage completed information. 
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param uri Location of the file to be copied
     * @param credentials Credentials data required to acces the source resource (file to be copied)
     * @param targetUri Location of the target directory to where the file is to be copied
     * @param targetCredentials Credentials data required to acces the destination resource
     * @param overwrite Overwrite target file if already exists
     * 
     * @return UUID DataAvenue assigned task id
     * 
     * @throws TicketException If the provided ticket is invalid/expired 
     * @throws NotSupportedProtocolException If the protocol is not supported by DataAvenue 
     * @throws URIException The given uri or targetUri is invalid (inaccessible, of invalid format)
     * @throws CredentialException If credentials are invalid or the ticket is invalid/expired
     * @throws OperationException If the given operation cannot be performed (e.g., the file already exists on target resource)
	 * @throws SessionExpiredException If the session expired (authentication data resubmit required)
     */
	@WebMethod public String move(@WebParam(name="ticket") String ticket, @WebParam(name="uri") String uri, @WebParam(name="credentials") CredentialAttributes creds, @WebParam(name="targetUri") String targetUriString, @WebParam(name="targetCredentials") CredentialAttributes targetCreds, @WebParam(name="overwrite") boolean overwrite) throws TicketException, URIException, OperationException, CredentialException, NotSupportedProtocolException, IOException, SessionExpiredException;
	
	/**
	 * Returns state and configuration details of the previously issued command.
	 * 
     * @param ticket Ticket that allows the use of DataAvenue
	 * @param id Task id
	 * 
	 * @return TaskDetails State the task
	 *  
     * @throws TicketException If the provided ticket is invalid/expired 
	 * @throws TaskIdException If the provied id is invalid/expired
     * @throws OperationException If the given operation cannot be performed
	 * @throws SessionExpiredException If session expired
	 * @throws CredentialException If too many parallel sessions
	 */
	@WebMethod public TransferDetails getState(@WebParam(name="ticket") String ticket, @WebParam(name="taskId") String id) throws TicketException, TaskIdException, OperationException, SessionExpiredException, CredentialException;
    
	/**
	 * Cancels a previously issued command.
	 * 
     * @param ticket Ticket that allows the use of DataAvenue
	 * @param id Task id
	 * 
     * @throws TicketException If the provided ticket is invalid/expired 
	 * @throws hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException If the provied id is invalid/expired
     * @throws OperationException If the given operation cannot be performed
	 * @throws SessionExpiredException If session expired
	 * @throws CredentialException If too many parallel sessions
	 */
	@WebMethod public void cancel(@WebParam(name="ticket") String ticket, @WebParam(name="taskId") String id) throws TicketException, TaskIdException, OperationException, SessionExpiredException, CredentialException;
	
    /**
     * Returns an ID of resource to download from/upload to that can be accessed via alias URL [http://host:port/webapp/]ID 
     * with get/put http operations.
     * NOTE: Credential data MUST be provided and valid at the time of download/upload. 
     * 
     * @param ticket Ticket that allows the use of DataAvenue
     * @param uri Location of the source file to be downloaded/uploaded
     * @param credentials Credentials required to acces the file
     * @param read Is the source file to be read/write
     * @param lifetime Lifetime of the alias in seconds
     * @param archive An archive file will be up- or downloaded
     * 
     * @return UUID (DataAvenue URL address postfix) created for the resource
     *  
     * @throws NotSupportedProtocolException If the protocol is not supported by DataAvenue
     * @throws URIException If the given uri is invalid (inaccessible, of invalid format)
     * @throws CredentialException If credentials are invalid or the ticket is invalid/expired
     * @throws OperationException If the given operation cannot be performed
     * @throws SessionExpiredException If the session expired (authentication data resubmit required)
     */
	@WebMethod public String createAlias(@WebParam(name="ticket") String ticket, @WebParam(name="uri") String uri, @WebParam(name="credentials") CredentialAttributes creds, @WebParam(name="read") boolean read, @WebParam(name="lifetime") int lifetime, @WebParam(name="archive") boolean archive) throws TicketException, URIException, CredentialException, NotSupportedProtocolException, OperationException, SessionExpiredException;
	
    /**
     * User tickets can be generated using administrator tickets.
     * This method returns a newly created user ticket. The e-mail address of the user is required parameter 
     * (company is inherited from admin, name defaults to '(generated ticket)').
     * 
     * @param ticket Administrator ticket
     * @param name Name of the user (optional)
     * @param company Company name of the new user (optional)
     * @param email E-mail address of the new user
     * 
     * @return Created ticket
     * 
     * @throws TicketException If the provided ticket is invalid/expired or not administrator 
     * @throws OperationException If the given operation cannot be performed
     * @throws SessionExpiredException If the session has expired (authentication data resubmit required)
     */
	@WebMethod public String createTicket(@WebParam(name="ticket") String ticket, @WebParam(name="name") String name, @WebParam(name="company") String company, @WebParam(name="email") String email) throws TicketException, OperationException, SessionExpiredException;
	
}