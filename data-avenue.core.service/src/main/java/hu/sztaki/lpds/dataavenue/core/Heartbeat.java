package hu.sztaki.lpds.dataavenue.core;

import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.persistence.EntityManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.interfaces.TransferStateEnum;

/*
 * This class sends heartbeats to database at a given frequency in order to verify which instances are alive or dead.
 * Heartbeat times use databases clock (to avoid clock synchronization problems).
 * Instances are maintained in "Instances" table.
 * Transfer states of dead instances are marked "aborted" in "ExtendedTransferMonitor" table periodically.
 */
public class Heartbeat {
	private static final Logger log = LoggerFactory.getLogger(Heartbeat.class);
	private ScheduledExecutorService heartbeatScheduler = null;
	private ScheduledExecutorService cleanupScheduler = null;

	private static final Heartbeat INSTANCE = new Heartbeat(); 
	public static Heartbeat getInstance() { return INSTANCE; } // called on tomcat shutdown
	private Heartbeat() {
		try {
			if (Configuration.HEARTBEAT_FREQUENCY > 0) {
				heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
				heartbeatScheduler.scheduleAtFixedRate(new HeartbeatTask(), Configuration.HEARTBEAT_FREQUENCY, Configuration.HEARTBEAT_FREQUENCY, TimeUnit.SECONDS);
			}

			if (Configuration.DEAD_INSTANCE_CLEANUP_FREQUENCY > 0) {
				cleanupScheduler = Executors.newSingleThreadScheduledExecutor();
				cleanupScheduler.scheduleAtFixedRate(new DetectAndCleanupDeadInstancesTask(), Configuration.HEARTBEAT_FREQUENCY + 1, Configuration.DEAD_INSTANCE_CLEANUP_FREQUENCY, TimeUnit.SECONDS);
			}
		} catch(Exception e) { log.warn("Cannot schedule transfer clean-up timer!"); }
	}
	
	public void shutdown() {
		log.info("Shutting down Hearbeat threads...");
		if (heartbeatScheduler != null && !heartbeatScheduler.isTerminated()) heartbeatScheduler.shutdownNow();
		if (cleanupScheduler != null && !cleanupScheduler.isTerminated()) cleanupScheduler.shutdownNow();
	}

	// this tasks sends heartbeat signals to database periodically
	static class HeartbeatTask extends TimerTask { 
		public void run() {
			try {
				EntityManager entityManager = DBManager.getInstance().getEntityManager();
				if (entityManager == null) { log.warn("No database connection"); return; }
	
				entityManager.getTransaction().begin();
				Instances instance = entityManager.find(Instances.class, Configuration.instanceId);
				if (instance == null) { // register instance
					entityManager.persist(new Instances());
					log.info("New DataAvenue instance registered with id: " + Configuration.instanceId);
				} else {
					if (instance.getState() != InstanceStateEnum.ALIVE) log.warn("Database thinks I am dead: " + Configuration.instanceId);
					long now = System.currentTimeMillis();
//					log.trace("Hearbeat: DataAvenue instance " + Configuration.instanceId + " is alive at " + now);
					instance.setTouched(now); // just to be sure there is an update of the row causing heartbeat value updated automatically
					instance.setState(InstanceStateEnum.ALIVE);
					instance.setDied(0l);
					entityManager.flush();
				}
				entityManager.getTransaction().commit();
				entityManager.close();
			} catch (Throwable x) {
				log.error("Cloud not send heartbeat", x);
			}
		}
	}

	// this task detects dead instances (too old heartbeat Configuration.instanceDeadThreshold) and sets abort state to ongoing transfers of dead instances
	static class DetectAndCleanupDeadInstancesTask extends TimerTask {
		public void run() {
			try {

				EntityManager entityManager = DBManager.getInstance().getEntityManager();
				if (entityManager == null) { log.warn("No database connection"); return; }	

				long now = System.currentTimeMillis();
				entityManager.getTransaction().begin();
				
				// for all ALIVE instances having heartbeat older than INSTANCE_DEAD_THRESHOLD -> DEAD
				// NOTE: this can be in a db stored procedure
				List<Instances> instances = entityManager.createNamedQuery("Instances.findDead", Instances.class)
						.setParameter("p", Configuration.INSTANCE_DEAD_THRESHOLD).getResultList();
				if (instances != null && instances.size() > 0) {
					for (Instances instance: instances) {
						instance.setDied(now);
						instance.setState(InstanceStateEnum.DEAD);
						log.info("DataAvenue instance is DEAD: " + instance.getInstanceId() + " (lived for: " + ((now - instance.getCreated()) / 1000) + "s)");
					}
				} else {
					// log.trace("No newly died DataAvenue instance detected");
				}
				
				instances = entityManager.createNamedQuery("Instances.findVeryDead", Instances.class )
						.setParameter("p", Configuration.DEAD_INSTANCE_CLEANUP_THRESHOLD).getResultList();
				if (instances != null && instances.size() > 0) {
					for (Instances instance: instances) {
						entityManager.remove(instance);
						log.info("Very DEAD DataAvenue instance: " + instance.getInstanceId() + " deleted, no hearbeat for " + Configuration.DEAD_INSTANCE_CLEANUP_THRESHOLD + "s (lived for: " + (now - instance.getCreated()) + " ms)");
						
						// searching for its ongoing transfers to abort...
						List<ExtendedTransferMonitor> deadTransfers = entityManager.createNamedQuery("ExtendedTransferMonitor.findAborted", ExtendedTransferMonitor.class )
								.setParameter("p", instance.getInstanceId()).getResultList();
						if (deadTransfers != null && deadTransfers.size() > 0) {
							for (ExtendedTransferMonitor deadTransfer: deadTransfers) {
								if (deadTransfer.getState() != TransferStateEnum.DONE &&
									deadTransfer.getState() != TransferStateEnum.FAILED &&
									deadTransfer.getState() != TransferStateEnum.CANCELED) {
									deadTransfer.setState(TransferStateEnum.ABORTED);
									deadTransfer.setFailureCause("Hosting DataAvenue instance " + instance.getInstanceId() +  " died");
									deadTransfer.setEnded(now);
									log.info("Status of transfer " + deadTransfer.getTaskId() + " set ABORTED");
								}
							}
						} // else log.debug("No ongoing transfers found for this very dead DataAvenue instance");
					}
				} // else log.trace("No very dead DataAvenue instances found");

				entityManager.getTransaction().commit();
				entityManager.close();

			} catch (Throwable x) {
				log.error("Cloud not send heartbeat", x);
			}
		}
	}
}
