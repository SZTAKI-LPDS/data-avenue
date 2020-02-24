package hu.sztaki.lpds.dataavenue.core;

import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.GSIFTP_PROTOCOL;
import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.SRM_PROTOCOL;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.Limits;
import hu.sztaki.lpds.dataavenue.interfaces.TransferMonitor;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DataAvenueSessionImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CopyDirTask extends CopyTask implements Callable<Void> {
	
	private static final Logger log = LoggerFactory.getLogger(CopyDirTask.class);

	CopyDirTask(final URIBase source, final Credentials sourceCredentials, final URIBase target, final Credentials targetCredentials, final TransferMonitor monitor, final boolean isMove, final boolean overwrite, final CopyTaskManager container, final String id) {
		super(source, sourceCredentials, target, targetCredentials, monitor, isMove, overwrite, container, id);
	}
	
	private long traverse(Adaptor adaptor, Credentials sourceCredentials, DataAvenueSession sourceSession, URIBase dirLocation, String targetBasePath, List<URIBase> dirs, Map<URIBase, URIBase> files) throws URIException, OperationException, CredentialException {
		log.trace("Calculating dir size of " + dirLocation.getURI() + "... ");
		long total = 0l;
		for (URIBase entry: adaptor.list(dirLocation, sourceCredentials, sourceSession)) { // it creates source session with source credentials
			switch (entry.getType()) {
				case DIRECTORY:
					log.trace("Subdir found: " + targetBasePath + "" + entry.getEntryName() + "/");
					dirs.add(new DefaultURIBaseImpl(targetBasePath + entry.getEntryName() + "/"));
					total += traverse(adaptor, null, sourceSession, entry, targetBasePath + entry.getEntryName() + "/", dirs, files);
					break;
				case FILE:
					files.put(entry, new DefaultURIBaseImpl(targetBasePath + entry.getEntryName()));
					try {
						long size = adaptor.getFileSize(entry, sourceCredentials, sourceSession);
						if (size > 0) total += size; 
					} catch (Exception e) { log.debug("Cannot query file size of: " + entry.getURI(), e); }
					break;
				default:
					log.debug("Entry not copied: " + entry + "");
					break;
			}
		}
		return total;
	}
	
	@Override public Void call() {
		if (isCanceled) { container.taskEnded(id); return null; } // task canceled in created or scheduled state
		
		DataAvenueSessionImpl sourceSession = null, targetSession = null;
		
		try { // generic error catcher
		
			log.trace("Copy dir task started...");
			monitor.transferring();
	
	        Adaptor sourceAdaptor = AdaptorRegistry.getAdaptorInstance(source.getProtocol());
	        Adaptor targetAdaptor = AdaptorRegistry.getAdaptorInstance(target.getProtocol());
	
			sourceSession = new DataAvenueSessionImpl(); // create new sessions (does not expire with user session) 
			targetSession = new DataAvenueSessionImpl(); 
	
			// traverse source dir recursively, collect files to be copied, directories to be created, total size
			List<URIBase> dirs = new LinkedList<URIBase>();
			Map<URIBase, URIBase> files = new HashMap<URIBase, URIBase>();
			long totalBytes = 0l;
			for (int i = 0; (i < Limits.MAX_ERRORS + 1)&& !isCanceled; i++) {
				try {
					totalBytes = traverse(sourceAdaptor, sourceCredentials, sourceSession, source, target.getURI() + source.getEntryName() + "/", dirs, files);
					break;
				} catch (OperationException x) {
					if (errors == Limits.MAX_ERRORS) throw x; else { errors++; monitor.retry(); try { Thread.sleep(Limits.ERROR_RETRY_DELAY); } catch (Exception z) {} monitor.transferring(); }
					log.debug("Retrying operation (errors: " + errors + ")");
					dirs.clear();
					files.clear();
				}
			}
			
			monitor.setTotalDataSize(totalBytes);
			
			log.trace("Copy: " + dirs.size() + " dirs, " + files.size() + " files, " + totalBytes + " bytes...");
			
			// create target root dir 
			for (int i = 0; (i < Limits.MAX_ERRORS + 1) && !isCanceled; i++) {
				try {
					log.trace("mkdir: " + target.getURI() + source.getEntryName() + "/");
					targetAdaptor.mkdir(new DefaultURIBaseImpl(target.getURI() + source.getEntryName() + "/"), targetCredentials, targetSession); // creates target credentials with target credentials
					break;
				} catch (OperationException x) {
					
					if (overwrite /* && x.getMessage().contains("exists")*/) { // FIXME if target dir already exists, that is not an error, on overwrite 
						break;
					} else {
						if (errors == Limits.MAX_ERRORS) throw x; else { errors++; monitor.retry(); try { Thread.sleep(Limits.ERROR_RETRY_DELAY); } catch (Exception z) {} monitor.transferring(); }
						log.debug("Retrying operation (errors: " + errors + ")");
					}
				}
			}
			// create target subdirs one-by-one...
			for (URIBase dir: dirs) {
				for (int i = 0; (i < Limits.MAX_ERRORS + 1) && !isCanceled; i++) {
					try {
						log.trace("mkdir: " + dir.getURI());
						targetAdaptor.mkdir(dir, null, targetSession);
						break;
					} catch (OperationException x) {
						if (overwrite /* && x.getMessage().contains("exists")*/) { // FIXME if target dir already exists, that is not an error, on overwrite 
							break;
						} else {
							if (errors == Limits.MAX_ERRORS) throw x; else { errors++; monitor.retry(); try { Thread.sleep(Limits.ERROR_RETRY_DELAY); } catch (Exception z) {} monitor.transferring(); }
							log.debug("Retrying operation (errors: " + errors + ")");
						}
					}
				}
			}
			
			// copy files one-by-one...
			for (URIBase sourceFileURI: files.keySet()) {
				if (isCanceled) break;
				
				URIBase destFileURI = files.get(sourceFileURI);
	
				if (!overwrite) { // if !overwrite check exists
					boolean readable = false;
					try { readable = targetAdaptor.isReadable(destFileURI, null, targetSession); } catch (Exception e) {}
					if (readable) throw new OperationException("Target file already exists: " + destFileURI.getURI());
				} else {
					// delete target if exists in the case of gsiftp/srm
		    		if (GSIFTP_PROTOCOL.equals(destFileURI.getProtocol()) || SRM_PROTOCOL.equals(destFileURI.getProtocol())) {
		    			log.debug("Trying to delete file {} (to allow multiple gsiftp/srm uploads...", destFileURI);
		    			try { targetAdaptor.delete(destFileURI, null, targetSession); } 
		    			catch (Exception e) { log.debug("Cannot delete file ({})", e.getMessage());	}
		    		}
		    		// s3 overwrites, so no deletion is necessary
				}
	
				// try to get file size and set in monitor
		        long fileSize = -1l;
		        try { 
		        	log.debug("Getting size of source file...");
		        	fileSize = sourceAdaptor.getFileSize(sourceFileURI, null, sourceSession);
				} catch (Exception e) { log.trace("No file size info available (" + e.getMessage() + ")"); } // silently ignore if source file size cannot be retrieved
				
		        doCopy(sourceAdaptor, targetAdaptor, sourceFileURI, destFileURI, null, null, sourceSession, targetSession, fileSize, monitor, this);
			}
			
			if (!isCanceled && isMove) {
				for (int i = 0; (i < Limits.MAX_ERRORS + 1) && !isCanceled; i++) {
					try {
						log.debug("Deleting source folder (move): " + source);
						sourceAdaptor.rmdir(source, null, sourceSession); 
						break;
					} catch (OperationException x) {
						if (errors == Limits.MAX_ERRORS) throw x; else { errors++; monitor.retry(); try { Thread.sleep(Limits.ERROR_RETRY_DELAY); } catch (Exception z) {} monitor.transferring(); }
						log.debug("Retrying operation (errors: " + errors + ")");
					}
				}
			}
	
			monitor.done();

		} catch (Exception e) { // generic error catcher
			log.trace("Copy dir exception", e);
			monitor.failed(e.getMessage()); 
		} 
		finally {
			log.trace("Discarding source and target sessions...");
			if (sourceSession != null) { try { ((DataAvenueSessionImpl)sourceSession).discard(); } catch(Exception e) {} }
			if (targetSession != null) { try { ((DataAvenueSessionImpl)targetSession).discard(); } catch(Exception e) {} }
		}
		
		container.taskEnded(id); // callback to remove this task from active tasks
		return null;
	}
}