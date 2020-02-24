package hu.sztaki.lpds.dataavenue.interfaces;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationNotSupportedException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * @author Akos Hajnal
 * 
 * Interface collecting synchronous (blocking) commands that adaptors must implement.
 *
 */
public interface SyncCommands {
    
    /**
     * Returns directory contents as list of {@link URIBase}s
     * 
     * @param uri Location of the directory
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @return List of directory entries (files/subdirectories)
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public List<URIBase> list(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException;
    
    /**
     * Returns attributes of a single directory or file
     * 
     * @param uri Location of the directory or file
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @return URIBase containing size, last modification date, etc. details
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public URIBase attributes(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException;

    /**
     * Returns list of subentry attributes for the specified subentries (subentries) within a specified directory (uri)
     * If no list provided (null or empty list), returns the whole directory contents with details as with list.
     * 
     * @param uri Location of the directory
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @param subentries List of subentires (file names, subdirectries) 
     * @return URIBase containing size, last modification date, etc. details
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession session, List <String> subentires) throws URIException, OperationException, CredentialException;
    
    /**
     * Creates the specified directory (and its parent directories, recursively, if absent).
     * 
     * @param uri Location of the new directory
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException;  
	
    /**
     * Removes the specified directory (and its contents, recursively).
     * 
     * @param uri Location of the directory
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException;  

    /**
     * Deletes the specified file.
     * 
     * @param uri Location of the file
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public void delete(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException;  

    /**
     * Sets permission for the specified file/directory.
     * 
     * @param uri Location of the file
     * @param credentials Credentials required to acces the resource
     * @param permissions Permissions string
     * 
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public void permissions(URIBase uri, Credentials credentials, DataAvenueSession session, String permissions) throws URIException, OperationException, CredentialException;  

    /**
     * Renames the specified file to a new name.
     * 
     * @param uri Location of the file
     * @param newName The new name of the file
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public void rename(URIBase uri, String newName, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException;  
	
    /**
     * Gets the input stream of the specified file for reading.
     * 
     * @param uri Location of the file
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @return The input stream of the file
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException; 

    /**
     * Gets the output stream of the specified file for writing. 
     * If the output file exists it will be overwritten (opened with non-appending mode).
     * The length of the content to be written should be known in advance. (To avoid reading the full stream into memory to determine size
     * for HTTP POST/PUT.)
     *  
     * @param uri Location of the file
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @param contentLength Content length to be written to output stream. -1 if unknown.
     * @return The output stream of the remote object
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public OutputStream getOutputStream(URIBase uri, Credentials credentials, DataAvenueSession session, long contentLength) throws URIException, OperationException, CredentialException; 

    /**
     * Writes remote object from input stream.
     * This method is invoked first (instead of directly getOutputStream) to write outputStream from inputStream by the adaptor itself.
     * This is necessary, because often it is not possible to get the outputStream of the remote object (when PUT/POST is used or the API
     * does not provide method to get outputStream).

     * The method might throw OperationNotSupportedException (which is the case well behaving storages) 
     * if the adaptor cannot write outputStream or cannot provide getOutputStream 
     * (which is the recommended way, as writeFromInputStream does not maintain progress information).
     * If OperationNotSupportedException is thrown, the transfer does not fail, but getOutputStream is called afterwards to continue. 
     * 
     * Throws OperationNotSupportedException if not supported by the adaptor.
     *  
     * @param uri Location of the file
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @param The input stream to be written to the output stream entirely
     * @param contentLength Content length to be written to output stream. -1 if unknown.
     * 
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     * @throws OperationNotSupportedException If this operation is not supported by the adaptor
     */
    public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session, InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException, OperationNotSupportedException; 
    
    /**
     * Returns the file size - required to allow monitoring and gather statistics about copy/move tasks.
     * Also required, when using PUT, POST so file size must be known in advance.
     * 
     * 
     * @return File size in bytes, -1 if unknown (not 0)
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws OperationException If the operation cannot be performed
     * @throws CredentialException If creadentials are invalid
     */
    public long getFileSize(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException;
    
    /**
     * Checks whether the specified file is readable (existence and read permission).
     * 
     * @param uri Location of the file
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @return true if the file is readable
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws CredentialException If creadentials are invalid
     * @throws OperationException If the operation cannot be performed
     */
    public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException;
    
    /**
     * Checks whether the specified file is writable (creation and write permission).
     * 
     * @param uri Location of the file
     * @param credentials Credentials required to access the recource
     * @param session {@link DataAvenueSession} provided by DataAvenue
     * @return true if the file is writable
     * @throws URIException If uri is inaccessible or of invalid format
     * @throws CredentialException If creadentials are invalid
     * @throws OperationException If the operation cannot be performed
     */
    public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException;
    
    // TODO isReadable, isWritable should be replaced by exists()
}