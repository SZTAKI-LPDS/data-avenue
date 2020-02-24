package hu.sztaki.lpds.dataavenue.adaptors.s3;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3CopyTaskRegistry {
	private static final Logger log = LoggerFactory.getLogger(S3CopyTaskRegistry.class);
	
	private static final S3CopyTaskRegistry INSTANCE = new S3CopyTaskRegistry(); 
	public static S3CopyTaskRegistry getInstance() { return INSTANCE; }
	private S3CopyTaskRegistry() {}
	
	private static final int MAXIMUM_NUMBER_OF_ACTIVE_COPY_THREADS = 50; // number of max simultaneous copy tasks
	private ExecutorService executor = Executors.newFixedThreadPool(MAXIMUM_NUMBER_OF_ACTIVE_COPY_THREADS); 
	
	// registry of active tasks
	private final ConcurrentHashMap<UUID, S3CopyTask> activeTasks = new ConcurrentHashMap<UUID, S3CopyTask> ();

	private void register(UUID id, S3CopyTask task) {
		if (activeTasks.put(id, task) != null) log.error("Another task with the same key registered");
	}

	private void deregister(UUID id) {
		if (activeTasks.remove(id) == null) log.error("No task registered with the given key");  
	}
	
	void submit(UUID id, S3CopyTask task) {
		register(id, task);
		Future <Void> future = executor.submit(task); // submit this task
		task.setFuture(future);
	}
	
	void finished(UUID id) {
		deregister(id);
	}
	
	void cancel(String idString) throws TaskIdException, OperationException {
		UUID id;
		try { id = UUID.fromString(idString); } catch(IllegalArgumentException e) { throw new TaskIdException(e); }
		if (!activeTasks.containsKey(id)) throw new TaskIdException("Invalid local id: " + id);
		S3CopyTask task = activeTasks.remove(id); // get and remove
		if (task != null) {  // if task is still active 
			task.cancel(); // cancel
		}
	}
}