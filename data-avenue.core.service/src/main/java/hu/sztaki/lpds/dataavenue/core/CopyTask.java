package hu.sztaki.lpds.dataavenue.core;

import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.GSIFTP_PROTOCOL;
import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.SRM_PROTOCOL;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.Limits;
import hu.sztaki.lpds.dataavenue.interfaces.TransferMonitor;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase.URIType;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationNotSupportedException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DataAvenueSessionImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.ogf.saga.error.AlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CopyTask implements Callable<Void> {
	private static final Logger log = LoggerFactory.getLogger(CopyTask.class);
	
	protected static final int DEFAULT_BUFFER_SIZE = 8 * 1024; // 8k

    protected final URIBase source; 
    protected URIBase target;
    protected final Credentials sourceCredentials, targetCredentials;
    protected final TransferMonitor monitor;
    protected final boolean isMove;
    protected final boolean overwrite;
    protected final CopyTaskManager container; // discard callback
    protected final String id; 

    protected Future<Void> future; // set on task submit by setFuture
    protected volatile boolean isCanceled = false; // is the task canceled?

    protected int errors = 0; // errors happened so far during performing the operation
    protected long bytesTransferredDuringFailures = 0l; // bytes erronously transferred so far during performing the operation
    
	CopyTask(final URIBase source, final Credentials sourceCredentials, final URIBase target, final Credentials targetCredentials, final TransferMonitor monitor, final boolean isMove, final boolean overwrite, final CopyTaskManager container, final String id) {
		this.source = source;
		this.target = target;
		this.sourceCredentials = sourceCredentials;
		this.targetCredentials = targetCredentials;
		this.monitor = monitor;
		this.isMove = isMove;
		this.overwrite = overwrite;
		this.container = container;
		this.id = id;
	}
	
	void setFuture(final Future<Void> future) { 
		this.future = future;
	}
	
	void cancel() {
		if (future != null) future.cancel(false); // don't interrupt if running
		this.isCanceled = true;
	}
	
	@Override public Void call() {
		if (!isCanceled) { // task not yet canceled during created or scheduled state
			
			log.debug("Copy/move task started (localid: " + id + ")");
			monitor.transferring();

			DataAvenueSessionImpl sourceSession = null, targetSession = null;
			
			try {
				Adaptor sourceAdaptor = AdaptorRegistry.getAdaptorInstance(source.getProtocol());
				
				sourceSession = new DataAvenueSessionImpl(); // create new sessions (does not expire with user session) 
				targetSession = new DataAvenueSessionImpl(); 

		        boolean isSourceSessionCreated = false; // if true don't pass credentials again

		        // try to get file size and set in monitor
		        long fileSize = -1l;
		        try { 
		        	log.debug("Getting size of source file...");
		        	fileSize = sourceAdaptor.getFileSize(source, sourceCredentials, sourceSession);
		        	isSourceSessionCreated = true;
		        	monitor.setTotalDataSize(fileSize);
		        	log.debug("Source object size: " + fileSize + " bytes"); 
				} catch (Exception e) { log.trace("No file size info available (" + e.getMessage() + ")"); } // silently ignore if source file size cannot be retrieved
		        
		        Adaptor targetAdaptor = AdaptorRegistry.getAdaptorInstance(target.getProtocol());

		        // determine whether target is a directory, and if yes, add / and filename if absent
		        String suffix = "";
		        if (target.getType() == URIType.URL || target.getType() == URIType.DIRECTORY) { 
		        	suffix = source.getEntryName();
		        	target = new DefaultURIBaseImpl(DefaultURIBaseImpl.getContainerDirUri(target) + suffix);
		        }

		        // if (overwrite == false) target file must not exist. 
		        boolean isTargetSessionCreated = false; // if true don't pass credentials again
		        if (!overwrite) {
		        	log.trace("Checking target is readable (not to overwrite)...");

		        	try { // note isWritable creates the file if it does not exist
		        		if (targetAdaptor.isReadable(target, targetCredentials, targetSession)) {
		        			log.info("Target already exists: " + target.getEntryName());
		        			// in this case abort copy
		        			throw new AlreadyExistsException("Target file already exists: " + target.getEntryName());
		        		} else {
		        			log.debug("Target does not exist");
		        		}

		        		isTargetSessionCreated = true;
		        	} catch (CredentialException | OperationException | URIException e) {
		        		// there was an exception, so the file prolly does not exist, if yes, the copy will fail for similar reason :)
		        		log.debug("Exception during readability check: " + e.getClass().getName() + " " + e.getMessage() + " ");
		        	} // silently ignore readability exceptions
		        } else {
		    		// gsiftp protocol allows write-once files. to allow multiple uploads, try to delete the file first
		    		if (GSIFTP_PROTOCOL.equals(target.getProtocol()) || SRM_PROTOCOL.equals(target.getProtocol())) {
		    			log.trace("Trying to delete file {} (multiple gsiftp/srm uploads)...", target);
		    			try { targetAdaptor.delete(target, targetCredentials, targetSession); } 
		    			catch (Exception e) { log.trace("File does not exists or cannot be deleted ({})", e.getMessage());	}
		    			isTargetSessionCreated = true;
		    		}
		        }

		        doCopy(sourceAdaptor, targetAdaptor, source, target, isSourceSessionCreated ? null : sourceCredentials, isTargetSessionCreated ? null : targetCredentials, sourceSession, targetSession, fileSize, monitor, this);
		        
		        log.info("Copy/move task ended, " + monitor.getBytesTransferred() + " bytes transferred");
		        if (!isCanceled) { 
		        	if (isMove) sourceAdaptor.delete(source, null, sourceSession); // do not add source credentails twice
		        	monitor.done();
		        }
			} 
			catch (Exception e) { 
				log.error("Copy task failed!", e);
				monitor.failed(e.getMessage() + (e.getCause() != null ? " (cause: " + e.getCause() + ")" : "")); 
			}
			catch (Throwable e) { // S3 throws uncaught exception
				log.error("Copy task failed! (Throwable)", e);
				monitor.failed(e.getMessage() + (e.getCause() != null ? " (cause: " + e.getCause() + ")" : "")); 
			}
			finally { 
				log.trace("Discarding source and target sessions...");
				if (sourceSession != null) { try { sourceSession.discard(); } catch(Exception e) {} }
				if (targetSession != null) { try { targetSession.discard(); } catch(Exception e) {} }
			}
		}
        container.taskEnded(id); // callback to remove this task from active tasks
		return null;
	}
	
	static void doCopy(Adaptor sourceAdaptor, Adaptor targetAdaptor, URIBase source, URIBase target, Credentials sourceCredentials, Credentials targetCredentials, DataAvenueSessionImpl sourceSession, DataAvenueSessionImpl targetSession, long fileSize, TransferMonitor monitor, CopyTask task) throws IOException, URIException, OperationException, CredentialException {
		
		log.trace("copy: " + source.getURI() + " -> " + target.getURI() + " size: " + fileSize + " bytes");
		
        // do with retry
		for (int i = 0; (i < Limits.MAX_ERRORS + 1) && !task.isCanceled; i++) {
			
			long tempBytesTransferredDuringFailure = 0l; 
			InputStream in = null;
			OutputStream out = null;
			try {
		        in = sourceAdaptor.getInputStream(source, sourceCredentials, sourceSession);
		        
		     	// try first writeFromInputStream
		       	boolean writeThroughStreamSuccessful = false;
		       	try { 
		       		targetAdaptor.writeFromInputStream(target, targetCredentials, targetSession, in, fileSize);
		       		writeThroughStreamSuccessful = true;
		       		log.trace("targetAdaptor.writeFromInputStream was successful");
			       	if (fileSize > 0) {
			       		monitor.incBytesTransferred(fileSize); // does not affect performance
			       		monitor.notifyBytesTransferredIncrement(fileSize); // does not affect performance
			       	}
		       	} 
		       	catch (OperationNotSupportedException x) {} // size out of range or not supported, skip
				catch (OperationException x) { throw new IOException(x); } // real error
		        	
		       	if (!writeThroughStreamSuccessful) {
			        out = targetAdaptor.getOutputStream(target, targetCredentials, targetSession, fileSize); 
			        byte [] buffer = new byte[DEFAULT_BUFFER_SIZE];
			        int readBytes;
				    while (!task.isCanceled && (readBytes = in.read(buffer)) > 0) { 
				      	out.write(buffer, 0, readBytes); 
				       	tempBytesTransferredDuringFailure += readBytes;
				       	monitor.incBytesTransferred(readBytes); // does not affect performance
				       	monitor.notifyBytesTransferredIncrement(readBytes); // does not affect performance
				    }
		       	}
		       	break;
			} catch (IOException x) {
				log.debug("IOException during copy: " + x.getMessage());
				task.bytesTransferredDuringFailures += tempBytesTransferredDuringFailure;
				if (task.bytesTransferredDuringFailures >= Limits.MAX_BYTES_TO_RETRANSFER) throw x;
				if (task.errors == Limits.MAX_ERRORS) throw x; else { task.errors++; monitor.retry(); try { Thread.sleep(Limits.ERROR_RETRY_DELAY); } catch (Exception z) {} monitor.transferring(); }
				log.debug("Retrying operation (errors: " + task.errors + ", bytesTransferredDuringFailures: " + task.bytesTransferredDuringFailures + ")");
			} catch (OperationException x) {
				log.debug("OperationException during copy: " + x.getMessage());
				task.bytesTransferredDuringFailures += tempBytesTransferredDuringFailure;
				if (task.bytesTransferredDuringFailures >= Limits.MAX_BYTES_TO_RETRANSFER) throw x;
				if (task.errors == Limits.MAX_ERRORS) throw x; else { task.errors++; monitor.retry(); try { Thread.sleep(Limits.ERROR_RETRY_DELAY); } catch (Exception z) {} monitor.transferring(); }
				log.debug("Retrying operation (errors: " + task.errors + ", bytesTransferredDuringFailures: " + task.bytesTransferredDuringFailures + ")");
			} finally {
				if (in != null) { try { in.close(); } catch (Exception e) {} }
				if (out != null) { try { out.close(); } catch (Exception e) {} }
			}
		}
	}
	
}