package hu.sztaki.lpds.dataavenue.interfaces;

import java.util.List;

/**
 * @author Akos Hajnal 
 * 
 * Interface that a DataAvenue adaptor must implement. 
 * Beyond the basic, descriptive information (adaptor name, desription, protocols, authentication types), an adaptor
 * implements synchronous and asynchronous operations (see {@link AsyncCommands}, {@link SyncCommands}).
 * 
 * Note: an adaptor may choose not to implement all the commands supported by the interface; {@link getSupportedOperationTypes} method 
 * indicates the list of realized functions.
 *
 */
public interface Adaptor extends AsyncCommands, SyncCommands {
	
	/**
	 * Returns the short name of the adaptor (e.g., "Gsiftp Adaptor")
	 * @return Adaptor short name
	 */
	public String getName(); 
	
	/**
	 * Returns the description of the adaptor (e.g., "Adaptor for accessing resources via the GridFTP protocol")
	 * @return Adaptor description
	 */
	public String getDescription(); 

	/**
	 * Returns the version (e.g., "0.1.1")
	 * @return Adaptor short name
	 */
	public String getVersion(); 
	
    /**
     * Returns the list of protocols (schemes) that the adaptor can handle (e.g., "gsiftp" for handling URLs starting with "gsiftp://")
     * @return Protocol list
     */
    public List<String> getSupportedProtocols();

    /**
     * Returns the list of operation types that the adaptor supports wrt. a given protocol.
     * This is a subset of operations: {LIST, MKDIR, RMDIR, DELETE, RENAME, PERMISSIONS, INPUT_STREAM, OUTPUT_STREAM}.
     * @return List of {@link OperationsEnum}
     */
    public List<OperationsEnum> getSupportedOperationTypes(String protocol);

    /**
     * NOTE: This is a specialized version of getSupportedOperationTypes(URIBase fromURI, URIBase toURI), thus excluded from the interface definition.
	 *
     * Returns the list of operation types that the adaptor supports between the two (adaptor-handled) protocols.
     * This is a subset of operations: {COPY_FILE, MOVE_FILE, COPY_DIR, MOVE_DIR}.
     * @return List of {@link OperationsEnum}
     * 
     * public List<OperationsEnum> getSupportedOperationTypes(String fromProtocol, String toProtocol);
     */

    /**
     * Returns the list of operation types that the adaptor supports between the two (adaptor-handled) URIs.
     * (E.g., an adaptor can handle copy within the same host, not not between hosts of the same protocol.)
     * This is a subset of operations: {COPY_FILE, MOVE_FILE, COPY_DIR, MOVE_DIR}, and .
     * @return List of {@link OperationsEnum}
     */
    public List<OperationsEnum> getSupportedOperationTypes(URIBase fromURI, URIBase toURI);
    
    /**
     * Returns the list of authentication types (security contexts) that the given protocol requires
     * Deprecated, use getAuthenticationTypeList instead of this.
     * 
     * @param protocol Protocol
     * @return List of authentication types
     */
    @Deprecated
    public List<String> getAuthenticationTypes(String protocol);  

    /**
     * Returns the list of authentication types (security contexts) that the given protocol requires
     * @param protocol Protocol
     * @return List of authentication types
     */
    public AuthenticationTypeList getAuthenticationTypeList(String protocol);  

    /**
     * Returns help string on what credential attributes (keys and values) must be provided for this authentication type
     * Deprecated, should be straightforward.
     * 
     * @param protocol
     * @param authenticationType
     * @return Authentication type usage help string
     */
    @Deprecated
    public String getAuthenticationTypeUsage(String protocol, String authenticationType);
    
    /*
     * Invoked by Data Avenue Core Services on shutdown. 
     * Resources allocated by the adaptors (e.g., thread pools) can be released here.
     */
    public void shutDown();
}