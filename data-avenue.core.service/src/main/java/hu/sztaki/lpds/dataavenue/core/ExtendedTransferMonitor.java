package hu.sztaki.lpds.dataavenue.core;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Transient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import hu.sztaki.lpds.dataavenue.interfaces.AsyncCommands;
import hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum;
import hu.sztaki.lpds.dataavenue.interfaces.TransferStateEnum;
import static hu.sztaki.lpds.dataavenue.interfaces.TransferStateEnum.*;
import hu.sztaki.lpds.dataavenue.interfaces.TransferMonitor;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;

/*
 * This class contains all details about a transfer (src, dest, state, size, ...), which is persisted in database on completion. 
 */
@NamedQueries({
@NamedQuery(
	    name="ExtendedTransferMonitor.findAll",
	    query="SELECT i FROM ExtendedTransferMonitor AS i ORDER BY i.created DESC"
),
@NamedQuery(
	    name="ExtendedTransferMonitor.findByUser",
	    query="SELECT i FROM ExtendedTransferMonitor i WHERE i.acknowledged = 0 AND i.ticket LIKE :p ORDER BY i.created DESC"
),
@NamedQuery(
	    name="ExtendedTransferMonitor.findExpired",
	    query="SELECT i FROM ExtendedTransferMonitor AS i WHERE i.created < :p"
),
@NamedQuery(
	    name="ExtendedTransferMonitor.findAborted",
	    query="SELECT i FROM ExtendedTransferMonitor i WHERE i.instanceId LIKE :p"
),
@NamedQuery(
	    name="ExtendedTransferMonitor.findRunning",
	    query="SELECT i FROM ExtendedTransferMonitor i WHERE i.state <= 2 OR i.state = 7 ORDER BY i.created DESC"
),
@NamedQuery(
	    name="ExtendedTransferMonitor.findCompleted",
	    query="SELECT i FROM ExtendedTransferMonitor i WHERE i.state > 2 AND i.state <> 7 ORDER BY i.created DESC"
)
})
@Entity
public class ExtendedTransferMonitor implements TransferMonitor {
	private static final Logger log = LoggerFactory.getLogger(ExtendedTransferMonitor.class);
	private long notifiedIntermediateBytesTransferred = 0l;
	
	public ExtendedTransferMonitor() {} // required by hybernate
	
	public ExtendedTransferMonitor(final String ticket, final String source, final String target, final OperationsEnum operation) {
		setTicket(ticket);
		setSource(source);
		setDestination(target);
		setOperation(operation);
		setInstanceId(Configuration.instanceId);
	}
	
	// TRANSER MONITOR INTERFACE IMPLEMENTATION
	private volatile long totalDataSize = 0l; // file size, 0 if unknown
	@Override public synchronized void setTotalDataSize(long size) {	this.totalDataSize = size; }
	@Override public synchronized long getTotalDataSize() { return totalDataSize;	}

	private volatile long bytesTransferred = 0l; 
	@Override public synchronized void setBytesTransferred(long bytes) { 
		this.bytesTransferred = bytes; 
	}	
	@Override public synchronized void incBytesTransferred(long bytes) { 
		this.bytesTransferred += bytes; 
	}	
	@Override public synchronized long getBytesTransferred() { return bytesTransferred; }

	private volatile String failureCause = null; // cause if FAILED or CANCELED
	@Override public synchronized String getFailureCause() { return failureCause; }
	@Override public synchronized void setFailureCause(String cause) { this.failureCause = DBManager.abbreviate(cause); }
	
	@Override public void notifyBytesTransferredIncrement(long bytesTransferredSinceLastNotification) {
		notifiedIntermediateBytesTransferred += bytesTransferredSinceLastNotification;
		Statistics.incBytesTransferred(bytesTransferredSinceLastNotification);
	}
	
	private volatile TransferStateEnum state = CREATED;
	public synchronized void setState(TransferStateEnum state) { this.state = state; }
	public synchronized TransferStateEnum getState() { return state;}

	@Override synchronized public void scheduled() { 
		setState(SCHEDULED); 
		log.debug("Task scheduled: " + this.getTaskId()); 
	}
	
	@Override synchronized public void transferring() { 	
		setState(TRANSFERRING); 
		started = System.currentTimeMillis(); 
		log.debug("Task transferring: " + this.getTaskId());
		TaskManager.getInstance().updateTransferMonitorState(this);
	}

	@Override synchronized public void done() {
		setState(DONE);
		setEnded(System.currentTimeMillis());
		if (getBytesTransferred() < getTotalDataSize()) setBytesTransferred(getTotalDataSize()); // if could not monitor progress, set tranferred to size on completion
		if (getBytesTransferred() > 0l && notifiedIntermediateBytesTransferred < getBytesTransferred()) Statistics.incBytesTransferred(getBytesTransferred() - notifiedIntermediateBytesTransferred); // inc global bytes transferred
		TaskManager.getInstance().detachTransferMonitor(getTaskId());
		log.debug("Task done: " + this.getTaskId());
	}
	
	@Override synchronized public void failed(String cause) {
		setState(FAILED); 
		setFailureCause(cause);
		setEnded(System.currentTimeMillis());
		if (getBytesTransferred() > 0l && notifiedIntermediateBytesTransferred < getBytesTransferred()) Statistics.incBytesTransferred(getBytesTransferred() - notifiedIntermediateBytesTransferred); // inc global bytes transferred
		TaskManager.getInstance().detachTransferMonitor(getTaskId());
		log.debug("Task failed: " + this.getTaskId());
	}
	
	@Override synchronized public void retry() { 
		setState(RETRY); 
		log.debug("Task retrying: " + this.getTaskId()); 
	}	
	
	// END INTERFACE IMPLEMENTATION

	public synchronized void canceled() throws TaskIdException, OperationException { // extra state callback invoked by TaskManager on user cancel
		if (getState() == CREATED || getState() == SCHEDULED || getState() == TRANSFERRING || getState() == RETRY) {
			setState(CANCELED);
			setFailureCause("User cancel");
			setEnded(System.currentTimeMillis());
			if (getBytesTransferred() > 0l && notifiedIntermediateBytesTransferred < getBytesTransferred()) { 
				Statistics.incBytesTransferred(getBytesTransferred() - notifiedIntermediateBytesTransferred); // inc global bytes transferred
			}
			
			log.debug("Removing transfer monitor: " + getTaskId());
			TaskManager.getInstance().detachTransferMonitor(getTaskId()); // removes transfer monitor from activeTasks with the given global id
			
			log.debug("Cancelling internal task: " + getInternalTaskId());
			log.debug("Removing local task, managing adaptor: " + getManagingAdaptor().getClass().getName() + ", internal task id: " + getInternalTaskId());			
			getManagingAdaptor().cancel(getInternalTaskId()); // remove first, then cancel FIXME getInternalTaskId seems to be deleted
			log.debug("Task canceled: " + this.getTaskId());
		} else log.warn("Task cannot be canceled: " + getState());
	}
	
	// OTHER TASK DETAILS
	private String ticket; 
	public String getTicket() { return ticket; }
	public void setTicket(String ticket) { this.ticket = ticket; }

	@Id
	private String taskId = UUID.randomUUID().toString();
	public String getTaskId() { return taskId; }
	public void setTaskId(String taskId) { this.taskId = taskId; }
	
	@Transient // don't persist
	private String internalTaskId; // adaptor managed task id (for cancel)
	public String getInternalTaskId() { return internalTaskId; }
	public void setInternalTaskId(String internalTaskId) { this.internalTaskId = internalTaskId; }

	@Transient // don't persist
	private AsyncCommands managingAdaptor; // to allow cancel
	public AsyncCommands getManagingAdaptor() { return managingAdaptor; }
	public void setManagingAdaptor(AsyncCommands managingAdaptor) { this.managingAdaptor = managingAdaptor;	}

	private volatile long created = System.currentTimeMillis(); 
	public long getCreated() { return created; }
	public void setCreated(long created) { this.created = created; }
	public String getCreatedString() { return Utils.dateString(created); }

	private volatile long started; 
	public synchronized void setStarted(long started) { this.started = started; }
	public synchronized long getStarted() { return started; }
	public String getStartedString() { return Utils.dateString(started); }

	private volatile long ended;  
	public synchronized void setEnded(long ended) { this.ended = ended; }
	public synchronized long getEnded() { return ended; }
	public String getEndedString() { return Utils.dateString(ended); }
	
	private OperationsEnum operation; // can be one of the adaptors supported operation types (COPY/MOVE)
	public void setOperation(OperationsEnum operation) { this.operation = operation; }
	public OperationsEnum getOperation() {	return operation; }

	@Lob
	private String source;
	public String getSource() { return source; }
	public void setSource(String source) { this.source = source; }
	
	@Lob
	private String destination; // target reserved
	public String getDestination() { return destination; }
	public void setDestination(String target) { this.destination = target; }
	
	private String instanceId; // host id performing the transfer 
	public String getInstanceId() { return instanceId; }
	public void setInstanceId(String p) { this.instanceId = p; }
	
	private Boolean acknowledged = false; // state of transfer has been acknowledged by the user (dont care anymore) 
	public Boolean getAcknowledged() { return acknowledged; }
	public void setAcknowledged(Boolean p) { this.acknowledged = p; }
	
	public static List<ExtendedTransferMonitor> getActiveTransfers() {
		try {
			List<ExtendedTransferMonitor> activeTransfers = null; 
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			entityManager.getTransaction().begin();
			activeTransfers = entityManager.createNamedQuery("ExtendedTransferMonitor.findRunning", ExtendedTransferMonitor.class).getResultList();
			entityManager.getTransaction().commit();
			entityManager.close();
			if (activeTransfers == null || activeTransfers.size() == 0) return Collections.<ExtendedTransferMonitor>emptyList();
			else return activeTransfers;
		} catch (Throwable x) {	return Collections.<ExtendedTransferMonitor>emptyList(); }
	}

	public static List<ExtendedTransferMonitor> getCompletedTransfers() {
		try {
			List<ExtendedTransferMonitor> activeTransfers = null; 
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			entityManager.getTransaction().begin();
			activeTransfers = entityManager.createNamedQuery("ExtendedTransferMonitor.findCompleted", ExtendedTransferMonitor.class).getResultList();
			entityManager.getTransaction().commit();
			entityManager.close();
			if (activeTransfers == null || activeTransfers.size() == 0) return Collections.<ExtendedTransferMonitor>emptyList();
			else return activeTransfers;
		} catch (Throwable x) {	return Collections.<ExtendedTransferMonitor>emptyList(); }
	}

}
