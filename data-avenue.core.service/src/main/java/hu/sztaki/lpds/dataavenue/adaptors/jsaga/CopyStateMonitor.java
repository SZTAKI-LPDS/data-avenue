package hu.sztaki.lpds.dataavenue.adaptors.jsaga;

import org.ogf.saga.session.Session;
import org.ogf.saga.task.State;

import java.util.Map;

import hu.sztaki.lpds.dataavenue.core.Utils;
import hu.sztaki.lpds.dataavenue.interfaces.TransferMonitor;

import org.ogf.saga.context.Context;
import org.ogf.saga.error.NoSuccessException;
import org.ogf.saga.error.NotImplementedException;
import org.ogf.saga.error.TimeoutException;
import org.ogf.saga.monitoring.Callback;
import org.ogf.saga.monitoring.Metric;
import org.ogf.saga.monitoring.Monitorable;
import org.ogf.saga.namespace.NSEntry;
import org.ogf.saga.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyStateMonitor implements Callback {

	private static final Logger log = LoggerFactory.getLogger(CopyStateMonitor.class);
	
	private final TransferMonitor monitor;
	private final Task<NSEntry, Void> task;
	private final String taskId;
	private final Map<String, Task<NSEntry, Void>> taskRegistry;
	private final Map<String, NSEntry> taskResourceRegistry;
	private final Session session;
	
	private long transferStartTime;

	CopyStateMonitor(final Task<NSEntry, Void> task, final String id, final Map<String, Task<NSEntry, Void>> taskRegistry, final Map<String, NSEntry> taskResourceRegistry, final TransferMonitor monitor, final Session session) {
		this.task = task;
		this.taskId = id;
		this.taskRegistry= taskRegistry; 
		this.taskResourceRegistry = taskResourceRegistry;
		this.monitor = monitor;
		this.session = session;
	}

	private void discardTask() {
		log.debug("Releasing resources of task: " + taskId + "...");
		taskRegistry.remove(taskId);
		NSEntry entry = taskResourceRegistry.remove(taskId);
		try { 
			entry.close();
			log.debug(" done");
		}
		catch (NoSuccessException e) { log.debug(" failed"); }	
		catch (NotImplementedException e) { log.debug(" failed"); } // sliently ignore close expcetions
		
		log.debug("Closing copy session...");
		if (session != null) { 
			try { session.close(); } catch(Exception x) { log.warn("Cannot close JSAGA session!", x); } 
		}
		
        System.gc(); // https://forge.in2p3.fr/projects/jsaga/wiki/Datastaging_and_multi-threading_on_CREAMCE
	}
	
	public boolean cb(Monitorable mt, Metric metric, Context ctx) {
		log.debug("State callback invoked...");
		try {
			State state = task.getState();
			log.debug("State: " + state);
			
			switch (state) {
				case NEW:
					monitor.scheduled();
					break;
				case RUNNING:
					monitor.transferring();
					transferStartTime = System.currentTimeMillis();
					break;
				case SUSPENDED: // should not happen
					break;
					
				case CANCELED: 
					discardTask();
					return false; 
					
				case FAILED:
					String cause = null;
					try { task.rethrow(); } 
					catch (Exception e) {
						cause = Utils.getExceptionTrace(e);
						log.debug("Task FAILED", e);
					} 
					monitor.failed(cause);
					discardTask();
					return false; 
					
				case DONE:
					monitor.done();
					discardTask();
					
			        long time = System.currentTimeMillis() - transferStartTime;
			        log.info("### Transfer Time: " + ((float)time/1000) + " (" + time + "ms)" + ", bytes: " + monitor.getBytesTransferred() + ", speed: " + ((float)monitor.getBytesTransferred()/(1024*1024))/((float)time/1000) + " MB/s");

					return false;
					
				default:
					// should not happen
					log.error("Invalid task state: " + state);
			}
		} 
		catch (NotImplementedException e) { log.warn("Cannot read metric: " + e.getMessage() + ""); return false; }
		catch (TimeoutException e) {} 
		catch (NoSuccessException e) {}
		
		return true;
	}
}