package hu.sztaki.lpds.dataavenue.adaptors.jsaga;

import hu.sztaki.lpds.dataavenue.interfaces.TransferMonitor;

import org.ogf.saga.context.Context;
import org.ogf.saga.error.AuthenticationFailedException;
import org.ogf.saga.error.AuthorizationFailedException;
import org.ogf.saga.error.DoesNotExistException;
import org.ogf.saga.error.NotImplementedException;
import org.ogf.saga.error.PermissionDeniedException;
import org.ogf.saga.monitoring.Callback;
import org.ogf.saga.monitoring.Metric;
import org.ogf.saga.monitoring.Monitorable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CopyProgressMonitor implements Callback {
	private static final Logger log = LoggerFactory.getLogger(CopyProgressMonitor.class);
	
	private final TransferMonitor monitor;

	private long prevBytesTransferred = 0l;
	
	CopyProgressMonitor(TransferMonitor monitor) { this.monitor = monitor; }

	public boolean cb(Monitorable mt, Metric metric, Context ctx) {
		try {
			String value = metric.getAttribute(Metric.VALUE); // returns bytes transferred as string
			  
			try {
				long bytesTransferred = Long.parseLong(value);
				monitor.setBytesTransferred(bytesTransferred); // strote bytes transferred in monitor
				// notify monitor about the increment
				if (prevBytesTransferred == 0l) {
					monitor.notifyBytesTransferredIncrement(bytesTransferred);
					prevBytesTransferred = bytesTransferred;
				} else {
					// delta
					if (prevBytesTransferred < bytesTransferred) {
						monitor.notifyBytesTransferredIncrement(bytesTransferred - prevBytesTransferred);
						prevBytesTransferred = bytesTransferred;
					} else {} // it should not happen
				}
				
			} catch (NumberFormatException e) { 
				log.warn("Cannot convert transferred bytes from string to long: " + value); 
			} 
		} 
		catch (NotImplementedException e) { log.warn("Cannot read metric: " + e.getMessage() + ""); return false; }
		catch (AuthorizationFailedException e) { log.warn("Cannot read metric: " + e.getMessage() + ""); return false; }
		catch (AuthenticationFailedException e) { log.warn("Cannot read metric: " + e.getMessage() + ""); return false; } 
		catch (PermissionDeniedException e) { log.warn("Cannot read metric: " + e.getMessage() + ""); return false; } 
		catch (DoesNotExistException e) { log.warn("Cannot read metric: " + e.getMessage() + ""); return false; }
//		catch (TimeoutException e) { } 
//		catch (NoSuccessException e) {}
//		catch (IncorrectStateException e) {}
		catch (Exception e) { // catch all exceptions
			log.warn("Cannot read metric: " + e.getMessage() + "");
		}
		
		return true; // re-register
	}
}