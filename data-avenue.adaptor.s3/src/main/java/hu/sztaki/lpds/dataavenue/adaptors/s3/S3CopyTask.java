package hu.sztaki.lpds.dataavenue.adaptors.s3;

import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.Limits;
import hu.sztaki.lpds.dataavenue.interfaces.TransferMonitor;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

/**
 * This class copies/moves a single object between two different S3 servers
 */
class S3CopyTask implements Callable<Void> {
	
	private static final Logger log = LoggerFactory.getLogger(S3CopyTask.class);
    private static final int DEFAULT_BUFFER_SIZE = 16384; // 16k buffer for copy

    private final S3URIImpl source; 
    private S3URIImpl target;
    private final Credentials sourceCredentials, targetCredentials;
    private final TransferMonitor monitor;
    private final boolean isMove;
    private final boolean overwrite;
    private final S3CopyTaskRegistry container; // discard callback
    private final UUID id; 

	private Future<Void> future; // set on task submit by setFuture
	
	private boolean isCanceled = false; // is the task canceled?

	S3CopyTask(final S3URIImpl source, final Credentials sourceCredentials, final S3URIImpl target, final Credentials targetCredentials, final TransferMonitor monitor, final boolean isMove, final boolean overwrite, final S3CopyTaskRegistry container, final UUID id) {
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
		if (future != null) future.cancel(true); // interrupt if running
		this.isCanceled = true;
	}
	
	@Override public Void call() {
		S3Clients clients = null;
		try {
			if (!isCanceled) { // task not yet canceled during created or scheduled state
				
				log.info("S3 copy task started");
				monitor.transferring();
				
				// if same S3 server and same credentials, create one client and use third-party copy
				boolean sameClient = true;
				if (!source.getHost().equals(target.getHost())) sameClient = false; 
				if (source.getPort() != null && source.getPort() != target.getPort()) sameClient = false;

				// legacy support for credentials
				if (sourceCredentials.getCredentialAttribute(S3Adaptor.ACCESS_KEY_CREDENTIAL) == null) sourceCredentials.putCredentialAttribute(S3Adaptor.ACCESS_KEY_CREDENTIAL, sourceCredentials.getCredentialAttribute(S3Adaptor.LEGACY_ACCESS_KEY_CREDENTIAL));
				if (sourceCredentials.getCredentialAttribute(S3Adaptor.SECRET_KEY_CREDENTIAL) == null) sourceCredentials.putCredentialAttribute(S3Adaptor.SECRET_KEY_CREDENTIAL, sourceCredentials.getCredentialAttribute(S3Adaptor.LEGACY_SECRET_KEY_CREDENTIAL));
				if (targetCredentials.getCredentialAttribute(S3Adaptor.ACCESS_KEY_CREDENTIAL) == null) targetCredentials.putCredentialAttribute(S3Adaptor.ACCESS_KEY_CREDENTIAL, targetCredentials.getCredentialAttribute(S3Adaptor.LEGACY_ACCESS_KEY_CREDENTIAL));
				if (targetCredentials.getCredentialAttribute(S3Adaptor.SECRET_KEY_CREDENTIAL) == null) targetCredentials.putCredentialAttribute(S3Adaptor.SECRET_KEY_CREDENTIAL, targetCredentials.getCredentialAttribute(S3Adaptor.LEGACY_SECRET_KEY_CREDENTIAL));
				
				// check same access keys for source and target
				String sourceAccessKey = sourceCredentials.getCredentialAttribute(S3Adaptor.ACCESS_KEY_CREDENTIAL);
				if (sourceAccessKey != null && !sourceAccessKey.equals(targetCredentials.getCredentialAttribute(S3Adaptor.ACCESS_KEY_CREDENTIAL))) sameClient = false;
				String sourceSecretKey = sourceCredentials.getCredentialAttribute(S3Adaptor.SECRET_KEY_CREDENTIAL);
				
				clients = new S3Clients().withClient(source, sourceAccessKey, sourceSecretKey);
				AmazonS3Client sourceClient = clients.get(source);
				
				if (sameClient) {
					
					log.debug("Same host " + (isMove?"move: ":"copy: ") +  source.getURI() + " -> " + target.getURI());
					
					// read source file size
					ObjectMetadata sourceMetadata = null;
					try { 
						sourceMetadata = sourceClient.getObjectMetadata(source.getBucketName(), source.getPathWithinBucket().substring(1));
						if (sourceMetadata != null) monitor.setTotalDataSize(sourceMetadata.getContentLength());
					} catch (AmazonS3Exception e) {
						throw new OperationException("Source file does not exist!"); 
					}

					// check destination exists if not overwrite
					if (overwrite == false) {
						try { 
							sourceClient.getObjectMetadata(target.getBucketName(), target.getPathWithinBucket().substring(1));
							throw new OperationException("Target file already exists!");
						} catch (AmazonS3Exception e) {} // it should happen 
					} 
	
					CopyObjectRequest copyObjRequest;
					copyObjRequest = new CopyObjectRequest(source.getBucketName(), source.getPathWithinBucket().substring(1), target.getBucketName(), target.getPathWithinBucket().substring(1));
						
					CopyObjectResult result = null;
					try { 
						result = sourceClient.copyObject(copyObjRequest);
						if (sourceMetadata != null) {
							monitor.setBytesTransferred(sourceMetadata.getContentLength());
							// notfify bytes transferred
							monitor.notifyBytesTransferredIncrement(sourceMetadata.getContentLength());
						}
					} catch (AmazonS3Exception e) { 
						throw new OperationException("Source file does not exist!"); 
					}
						
					if (result == null) {
						throw new OperationException("Couldn't copy file!");
					} else {
						if (isMove) sourceClient.deleteObject(new DeleteObjectRequest(source.getBucketName(), source.getPathWithinBucket().substring(1))); // delete original object, omit first slash
						monitor.done();
					}
					
				} else { // not the same client (different hosts)
					
					log.debug((isMove?"move: ":"copy: ") +  source.getURI() + " -> " + target.getURI() + " overwrite: " + overwrite);
					clients.add(target, targetCredentials.getCredentialAttribute("UserID"), targetCredentials.getCredentialAttribute("UserPass"));
					AmazonS3Client targetClient = clients.get(target);
					
					// read file size
					long size = 0;
					try { 
						ObjectMetadata omd = sourceClient.getObjectMetadata(source.getBucketName(), source.getPathWithinBucket().substring(1));
						if (omd != null) {
							monitor.setTotalDataSize(omd.getContentLength());
							size = omd.getContentLength();
						}
					} catch (AmazonS3Exception e) {
						throw new OperationException("Source file does not exist!");
					}
					
					// check destination exists if not overwrite
					if (overwrite == false) {
						try { 
							targetClient.getObjectMetadata(target.getBucketName(), target.getPathWithinBucket().substring(1));
							throw new OperationException("Target file already exists!");
						} catch (AmazonS3Exception e) {} // target does not exist, which is expected
					} 
					
				    int errors = 0; // errors happened so far during performing the operation
				    long totalBytesTransferredDuringFailures = 0l; // bytes erronously transferred so far during performing the operation
					
			        // do with retry
					for (int i = 0; (i < Limits.MAX_ERRORS + 1) && !isCanceled; i++) {
						long currentBytesTransferredDuringFailure = 0l; 
						InputStream in = null;
						OutputStream out = null;
						try {
							// use streaming
							S3Object inObject = sourceClient.getObject(new GetObjectRequest(source.getBucketName(),  source.getPathWithinBucket().substring(1)));
							if (inObject == null) throw new OperationException("Amazon S3 constraint violation!"); // should not happen
							in = new BufferedInputStream(inObject.getObjectContent());

							// use putobjectrequest if size < PUT_OBJECT_LIMIT
					       	boolean writeThroughStreamSuccessful = false;
						    if (0 <= size && size <= PutObjectOutputStream.PUT_OBJECT_LIMIT) {
						       	try { 
						       		PutObjectOutputStream.writeInputStream(targetClient, target.getBucketName(), target.getPathWithinBucket().substring(1), in, size);
						       		writeThroughStreamSuccessful = true;
						       		// cannot get size of transferred bytes on failure
						       		monitor.setBytesTransferred(size);  
									monitor.notifyBytesTransferredIncrement(size);
						       	} 
								catch (RuntimeException x) { // AmazonClientException, AmazonServiceException
									throw new IOException(x);
								}
					       	}
					        	
					       	if (!writeThroughStreamSuccessful) {
								// use stream copy
					       		out = S3Adaptor.getOutputStreamForSize(targetClient, target.getBucketName(), target.getPathWithinBucket().substring(1), size);
								byte [] buffer = new byte[DEFAULT_BUFFER_SIZE];
							    int readBytes;
							    long totalBytes = 0l;
							    while (!isCanceled && (readBytes = in.read(buffer)) > 0) { 
							     	out.write(buffer, 0, readBytes);
							        totalBytes += readBytes;
							        currentBytesTransferredDuringFailure += readBytes;
							        monitor.setBytesTransferred(totalBytes);
									monitor.notifyBytesTransferredIncrement(readBytes);
						        }
					       	}
						    break;
						} catch (IOException x) {
							log.debug("IOException during copy: " + x.getMessage());
							totalBytesTransferredDuringFailures += currentBytesTransferredDuringFailure;
							if (totalBytesTransferredDuringFailures >= Limits.MAX_BYTES_TO_RETRANSFER) throw x;
							if (errors == Limits.MAX_ERRORS) throw x; else { errors++; monitor.retry(); try { Thread.sleep(Limits.ERROR_RETRY_DELAY); } catch (Exception z) {} monitor.transferring(); }
							log.debug("Retrying operation (errors: " + errors + ", bytesTransferredDuringFailures: " + totalBytesTransferredDuringFailures + ")");
						} finally {
							if (in != null) { try { in.close(); } catch (Exception e) {} }
							if (out != null) { try { out.close(); } catch (Exception e) {} }
						}
					}
					
			        if (!isCanceled) { 
						if (isMove) sourceClient.deleteObject(new DeleteObjectRequest(source.getBucketName(), source.getPathWithinBucket().substring(1))); // delete original object
			        	monitor.done();
			        }
				}
			} // if not cancelled
		} catch (Exception e) { monitor.failed(e.getMessage() + (e.getCause() != null ? " (" + e.getCause() + ")" : "")); }
		finally { if (clients != null) clients.close(); }
		
        container.finished(id); // remove this task from active tasks
		return null;
	}
}