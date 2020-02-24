package hu.sztaki.lpds.dataavenue.core;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.interfaces.AsyncCommands;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.TransferMonitor;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase.URIType;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

public class CopyTaskManager implements AsyncCommands { // singleton
	
	private static final Logger log = LoggerFactory.getLogger(CopyTaskManager.class);

	private static final CopyTaskManager INSTANCE = new CopyTaskManager(); 
	public static CopyTaskManager getInstance() { return INSTANCE; } // called on tomcat shutdown
	private CopyTaskManager() {}

	private final CopyTaskThreadPool executor = new CopyTaskThreadPool(); // executor for copy tasks, composition
	// tasks being submitted: map of: local task id -> task; stored to allow canceling tasks
	private volatile ConcurrentHashMap<String, CopyTask> activeTasks = new ConcurrentHashMap<String, CopyTask> ();
	
	// remove active tasks from map when finished (callback given to tasks)
	void taskEnded(final String id) {
		if (activeTasks.remove(id) != null) log.trace("CopyTaskManager task removed (task ended). Local id: " + id);
		else log.warn("CopyTaskManager task removed (not yet contained by active tasks!). Local id: " + id);
	}
	
	@Override public void cancel(final String id) throws TaskIdException, OperationException {
		if (id == null || !activeTasks.containsKey(id)) {
			log.warn("Invalid local process id: " + id + " (Task removed on ended?)");
			return;
		} 
		CopyTask task = activeTasks.remove(id); // get and remove
		if (task != null) task.cancel(); // if task is still active
		log.trace("CopyTaskManager task removed. Local id: " + id);
	}
	
	@Override public String copy(final URIBase fromUri, final Credentials fromCredentials, final URIBase toUri, final Credentials toCredentials, final boolean overwrite, final TransferMonitor monitor)
			throws URIException, OperationException, CredentialException {
		
		log.debug("From URI: " + fromUri.getURI());
		log.debug("To URI: " + toUri.getURI());
		log.debug("Overwrite: " + overwrite);

		String localId = UUID.randomUUID().toString();
		CopyTask task = 
				fromUri.getType() == URIType.DIRECTORY ? 
				new CopyDirTask(fromUri, fromCredentials, toUri, toCredentials, monitor, false, overwrite, this, localId) :
				new CopyTask(fromUri, fromCredentials, toUri, toCredentials, monitor, false, overwrite, this, localId);
				
		activeTasks.put(localId, task);
		log.trace("New CopyTaskManager " + localId);
		
		monitor.scheduled(); // indicate that this task is in POOLED state
		Future <Void> future = executor.poolTask(task); // submit this task
		task.setFuture(future); // store Future in task to allow of cancelling (in JDK7 removes from queue)
		return localId;
	}

	@Override public String move(final URIBase fromUri, final Credentials fromCredentials, final URIBase toUri, final Credentials toCredentials, final boolean overwrite, final TransferMonitor monitor)
			throws URIException, OperationException, CredentialException {

		String localId = UUID.randomUUID().toString();
		
		CopyTask task = 
				fromUri.getType() == URIType.DIRECTORY ?
				new CopyDirTask(fromUri, fromCredentials, toUri, toCredentials, monitor, true, overwrite, this, localId) :
				new CopyTask(fromUri, fromCredentials, toUri, toCredentials, monitor, true, overwrite, this, localId);
		
		activeTasks.put(localId, task);
		log.trace("New CopyTaskManager " + localId);
		monitor.scheduled(); // indicate that this task is in POOLED state
		Future <Void> future = executor.poolTask(task); // submit this task
		task.setFuture(future); // store Future in task to allow of cancelling it (in JDK7 removes from queue)
		return localId;
	}
	
	public void shutdown() {
		log.info("Shutting down CopyTaskManager ...");
		executor.shutdownNow();
	}
	
	private static class CopyTaskThreadPool {
		
		private static final Logger log = LoggerFactory.getLogger(CopyTaskThreadPool.class);
		private ExecutorService executor = Executors.newFixedThreadPool(Configuration.MAXIMUM_NUMBER_OF_ACTIVE_TRANSFER_THREADS); 
		
		// private CopyTaskThreadPool() { executor.setRemoveOnCancelPolicy(true); } TODO: JDK7, setRemoveOnCancelPolicy(true), remove task from queue
		
		private Future<Void> poolTask(CopyTask task) { return executor.submit(task); }

		void shutdownNow() { try { executor.shutdownNow(); } catch (Exception e) {} } // silently ignore shutwon exception
		
		@SuppressWarnings("unused")
		void shutdown(final int timeoutInSec) {	
			executor.shutdown(); // disable new tasks from being submitted
			try {
				if (!executor.awaitTermination(timeoutInSec, TimeUnit.SECONDS)) { // wait a while for existing tasks to terminate
					executor.shutdownNow(); // cancel currently executing tasks
					if (!executor.awaitTermination(timeoutInSec, TimeUnit.SECONDS)) // wait a while for tasks to respond to being cancelled
						log.error("CopyThreadPool did not terminate");
				}
			} catch (InterruptedException ie) {
				executor.shutdownNow(); // (re-)cancel if current thread also interrupted
				Thread.currentThread().interrupt(); // preserve interrupt status
			}
		}	
	}
}