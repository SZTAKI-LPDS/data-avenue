package hu.sztaki.lpds.dataavenue.core;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class contains all details about a instances (containers) used when many instances are serving requests
 * in load balanced way.
 * Details include unique instance id, private IP, state (ALIVE/DEAD/REMOVED), and heartbeat timestamp.  
 */

// named queries
@NamedQueries({
@NamedQuery(
	    name="Instances.findAll",
	    query="SELECT i FROM Instances AS i ORDER BY i.created DESC"
),
@NamedQuery(
	    name="Instances.findAlive",
	    query="SELECT i FROM Instances AS i WHERE i.state = 0"
),
@NamedQuery(
	    name="Instances.findDead",
 	    query="SELECT i FROM Instances AS i WHERE i.state = 0 AND TIMESTAMPDIFF(second, i.heartbeat, CURRENT_TIMESTAMP) > :p"
),
@NamedQuery(
	    name="Instances.findVeryDead",
	    query="SELECT i FROM Instances AS i WHERE i.state > 0 AND TIMESTAMPDIFF(second, i.heartbeat, CURRENT_TIMESTAMP) > :p"
)
})
// List<Instances> reslist = entityManager.createNamedQuery("Instances.findDeat", Instances.class)..setParameter("p", Configuration.instanceDeadThreshold).getResultList();

// the persistent class
@Entity
public class Instances {
	private static final Logger log = LoggerFactory.getLogger(Instances.class);
	
	public Instances() {
		setInstanceId(Configuration.instanceId);
		setPrivateIp(Configuration.privateIP);
	}

	@Id
	private String instanceId;
	public String getInstanceId() { return instanceId; }
	public void setInstanceId(String p) { this.instanceId = p; }
	
	private String privateIp;
	public String getPrivateIp() { return privateIp; }
	public void setPrivateIp(String p) { this.privateIp = p; }

	private volatile InstanceStateEnum state = InstanceStateEnum.ALIVE;
	public synchronized void setState(InstanceStateEnum p) { this.state = p; }
	public synchronized InstanceStateEnum getState() { return state;}	

	private volatile long created = System.currentTimeMillis(); 
	public long getCreated() { return created; }
	public void setCreated(long created) { this.created = created; }
	public String getCreatedString() { return Utils.dateString(created); }

	// update of this field implies updating hearbeat field with server-side timestamp
	private volatile long touched = 0l; 
	public long getTouched() { return touched; }
	public void setTouched(long p) { this.touched = p; }

	private volatile long died = 0l;  
	public synchronized void setDied(long p) { this.died = p; }
	public synchronized long getDied() { return died; }

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "heartbeat", updatable = false, insertable = false, columnDefinition="TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
	private Date heartbeat;
	public Date getHeartbeat() { return heartbeat; }
	public void setHeartbeat(Date heartbeat) { this.heartbeat = heartbeat; }
	
	public static List<Instances> getInstances() {
		try {
			List<Instances> instances = null; 
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			entityManager.getTransaction().begin();
			instances = entityManager.createNamedQuery("Instances.findAll", Instances.class).getResultList();
			entityManager.getTransaction().commit();
			entityManager.close();
			if (instances == null || instances.size() == 0) return Collections.<Instances>emptyList();
			else return instances;
		} catch (Throwable x) {	return Collections.<Instances>emptyList(); }
	}

	// [0] ip, [1] id, [2] tasks
	public static List<Object[]> getInstancesLoad() {
		try {
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			entityManager.getTransaction().begin();
			String query = "SELECT i.privateIp, i.instanceId as Server, COUNT(t.taskId) AS Tasks FROM Instances as i LEFT JOIN (SELECT * FROM ExtendedTransferMonitor WHERE state = 2) AS t ON i.instanceId = t.instanceId GROUP BY Server";
			@SuppressWarnings("unchecked")
			List<Object[]> resultSet = entityManager.createNativeQuery(query).getResultList();
			entityManager.getTransaction().commit();
			entityManager.close();
			if (resultSet == null || resultSet.size() == 0) return Collections.<Object[]>emptyList();
			else return resultSet;
		} catch (Throwable x) {	log.warn("Cannot collect instances load", x); return Collections.<Object[]>emptyList(); }
	}
}
