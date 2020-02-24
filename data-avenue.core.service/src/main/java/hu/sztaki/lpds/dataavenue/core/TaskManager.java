package hu.sztaki.lpds.dataavenue.core;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.persistence.EntityManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TaskManager {
	private static final Logger log = LoggerFactory.getLogger(TaskManager.class);
	private ScheduledExecutorService scheduler = null;

	private static final TaskManager INSTANCE = new TaskManager(); // singleton
	public static TaskManager getInstance() { return INSTANCE; }
	private TaskManager() {
		try {
			scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.scheduleAtFixedRate(new ExpiredTransfersCleanUpTask(), 0, Configuration.TRANSFER_CLEANUP_PERIOD, TimeUnit.SECONDS);
		} catch(Exception e) { log.error("Cannot schedule transfer clean-up timer!"); }
	}
	
	public void shutdown() {
		log.info("Shutting down transfer clean-up timer...");
		if (scheduler != null) scheduler.shutdownNow();
	}
	// task id -> transfer monitor
	private static final ConcurrentHashMap<String, ExtendedTransferMonitor> activeTransfers = new ConcurrentHashMap<String, ExtendedTransferMonitor> ();
	
	public void registerTransferMonitor(final ExtendedTransferMonitor transferMonitor) {
		activeTransfers.put(transferMonitor.getTaskId(), transferMonitor);
		log.trace("New active transfer monitor (id: " + transferMonitor.getTaskId() + ", local id: " + transferMonitor.getInternalTaskId() + ")");
	}
	
	public void cancelTransfer(final String id) throws TaskIdException, OperationException {
		if (id == null) throw new TaskIdException("No data found for transfer: " + id);
		ExtendedTransferMonitor transfer = activeTransfers.get(id);
		if (transfer != null) transfer.canceled(); // it detaches transfer monitor
		else {
			TransferDetails status = getTaskDetails(id); // throws TaskIdException if invalid
			throw new OperationException("Transfer cannot be cancelled with status: " + status.getState());
		}
	}

	// sets transfer state acknowledged
	public void acknowledgeTransfer(final String id) throws TaskIdException, OperationException {
		if (id == null) throw new TaskIdException("Transfer id is null");
		ExtendedTransferMonitor transfer = activeTransfers.get(id);
		if (transfer != null) {
			transfer.setAcknowledged(true);
		} else {
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			if (entityManager != null) {
				entityManager.getTransaction().begin();
				ExtendedTransferMonitor entity = entityManager.find(ExtendedTransferMonitor.class, id);
				if (entity != null) {
					entity.setAcknowledged(true);
					entityManager.getTransaction().commit();
					entityManager.close();
				} else {
					entityManager.getTransaction().commit();
					entityManager.close();
					throw new TaskIdException("Transfer not found: " + id);
				}
			} else log.warn("No database connection"); 
		}
	}
	
	// persists/updates state of the transfer monitor in the database 
	public void updateTransferMonitorState(final ExtendedTransferMonitor transfer) {
		log.trace("Updating transfer state: " + transfer.getState() + " " + transfer.getTaskId());
		// persist transfer
		String id = transfer.getTaskId();
		EntityManager entityManager = DBManager.getInstance().getEntityManager();
		if (entityManager != null) {
			entityManager.getTransaction().begin();
			ExtendedTransferMonitor entity = entityManager.find(ExtendedTransferMonitor.class, id);
			if (entity == null) {
				entityManager.persist(transfer); 
				entityManager.flush();
				log.trace("Transfer monitor persisted: " + transfer.getTaskId());
			} else { // update 
				entity.setState(transfer.getState());
				entity.setStarted(transfer.getStarted());
				entity.setTotalDataSize(transfer.getTotalDataSize());
				log.trace("Transfer monitor updated: " + transfer.getTaskId());
			}
			entityManager.getTransaction().commit();
			entityManager.close();
		}
	}
	
	public void detachTransferMonitor(final String id) { // called by monitor on task completion (remove from memory and persist)
		log.trace("Detaching transfer: " + id);
		ExtendedTransferMonitor transfer = activeTransfers.get(id);
		if (transfer != null) {
			// persist transfer
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			if (entityManager != null) {
				entityManager.getTransaction().begin();
				ExtendedTransferMonitor entity = entityManager.find(ExtendedTransferMonitor.class, id);
				if (entity == null) { // if new, persist
					entityManager.persist(transfer); 
					entityManager.flush();
					log.trace("Transfer monitor persisted: " + transfer.getTaskId());
				} else {
					// update all fields might be changed
					entity.setState(transfer.getState());
					entity.setStarted(transfer.getStarted());
					entity.setEnded(transfer.getEnded());
					entity.setBytesTransferred(transfer.getBytesTransferred());
					entity.setFailureCause(transfer.getFailureCause());
					entity.setTotalDataSize(transfer.getTotalDataSize());
					log.trace("Transfer monitor updated: " + transfer.getTaskId());
				}
				entityManager.getTransaction().commit();
				entityManager.close();
				// remove, after persisted
				activeTransfers.remove(id);
				log.trace("Transfer monitor removed: " + id);
			} else {} // keep transfer in memory
		} else log.warn("No transfer: " + id);
	}
	
	public TransferDetails getTaskDetails(final String id) throws TaskIdException {
		if (id == null) throw new TaskIdException("No data found for transfer: " + id);
		ExtendedTransferMonitor monitor = activeTransfers.get(id);
		if (monitor != null) { // if the task is active, read from map
//			log.debug("Reading active transfer details from memory...");
			log.debug("Transfer {} {} {}/{}", monitor.getTaskId(), monitor.getState(), monitor.getBytesTransferred(), monitor.getTotalDataSize());
			return new TransferDetails(monitor);
		} else { // read data from database
			log.debug("Reading transfer details from database...");
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			if (entityManager != null) {
				entityManager.getTransaction().begin();
				ExtendedTransferMonitor entity = entityManager.find(ExtendedTransferMonitor.class, id);
				if (entity == null) {
					entityManager.getTransaction().commit();
					entityManager.close();
					throw new TaskIdException("No data found for transfer: " + id);
				}
				entityManager.getTransaction().commit();
				entityManager.close();
				return new TransferDetails(entity);
			} else throw new RuntimeException("No database connection"); 
		}
	}

	public class ExtendedTransferMonitorComparator implements Comparator<ExtendedTransferMonitor> {
	    @Override public int compare(ExtendedTransferMonitor p1, ExtendedTransferMonitor p2) {
	    	return p1.getCreated() == p2.getCreated() ? 0 :
	    			(p1.getCreated() > p2.getCreated() ? 1 : -1); 
	    }
	}
	
	// get non-acknowledged tasks
	public Collection<ExtendedTransferMonitor> getAllNonAcknowledgedUserTranferDetails(final String ticket) {
		List <ExtendedTransferMonitor> tasks = new Vector<>();
		
		for (ExtendedTransferMonitor monitor: activeTransfers.values()) { 
			if (monitor.getTicket().equals(ticket) && monitor.getAcknowledged() == false) tasks.add(monitor);
		}
		
		EntityManager entityManager = DBManager.getInstance().getEntityManager();
		if (entityManager != null) {
			entityManager.getTransaction().begin();
			List<ExtendedTransferMonitor> userMonitors = entityManager.createNamedQuery("ExtendedTransferMonitor.findByUser", ExtendedTransferMonitor.class ).setParameter("p", ticket)./*setMaxResults(100).*/getResultList();
			if (userMonitors != null && userMonitors.size() > 0) {
				for (ExtendedTransferMonitor monitor: userMonitors) 
					tasks.add(monitor);
			}
			entityManager.getTransaction().commit();
			entityManager.close();
		}

//		tasks.sort(Comparator.comparing(p -> p.getKey())); 1.8+
		Collections.sort(tasks, new ExtendedTransferMonitorComparator());
//		Collections.reverse(tasks);
		log.debug("...returning monitor details");
		return tasks;
	}
	
	public Collection<ExtendedTransferMonitor> getAllTransferMonitors() {
		log.debug("...getting all transfer monitors");
		TreeMap<Long, ExtendedTransferMonitor> tasks = new TreeMap<Long, ExtendedTransferMonitor>(Collections.reverseOrder());
		
		log.debug("...getting activeTransfers ({})" + activeTransfers.size());
		for (ExtendedTransferMonitor monitor: activeTransfers.values()) 
			tasks.put(monitor.getCreated(), monitor);
		
		log.debug("...getting monitors from db");
		EntityManager entityManager = DBManager.getInstance().getEntityManager();
		if (entityManager != null) {
			entityManager.getTransaction().begin();
			List<ExtendedTransferMonitor> allMonitors = entityManager.createNamedQuery("ExtendedTransferMonitor.findAll", ExtendedTransferMonitor.class ).setMaxResults(100).getResultList();
			if (allMonitors != null && allMonitors.size() > 0) {
				for (ExtendedTransferMonitor monitor: allMonitors) 
					tasks.put(monitor.getCreated(), monitor);
			}
			entityManager.getTransaction().commit();
			entityManager.close();
		}

		log.debug("...returning monitor details");
		return tasks.values();
	}

	class ExpiredTransfersCleanUpTask extends TimerTask {
		public void run() {
			long now = System.currentTimeMillis();
			log.trace("Cleaning-up expired transfers... (cycle: " + Configuration.TRANSFER_CLEANUP_PERIOD + "s, created before: " + Utils.dateString(now) + ")");
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			if (entityManager != null) {
				entityManager.getTransaction().begin();
				
				// delete expired transfers
				List<ExtendedTransferMonitor> expiredTransfers = entityManager.createNamedQuery("ExtendedTransferMonitor.findExpired", ExtendedTransferMonitor.class ).setParameter("p", now - Configuration.TRANSFER_EXPIRATION_TIME * 1000l).getResultList();
				if (expiredTransfers != null && expiredTransfers.size() > 0) {
					log.info("Deleting " + expiredTransfers.size() + " transfer rows created before: " + Utils.dateString(now - Configuration.TRANSFER_EXPIRATION_TIME * 1000l) + "...");
					for (ExtendedTransferMonitor expired: expiredTransfers) entityManager.remove(expired);
				} else log.trace("No expired transfers");
				
				// delete expired instances done in Heartbeat.java
				
				entityManager.getTransaction().commit();
				entityManager.close();
			}
		}
	}
}