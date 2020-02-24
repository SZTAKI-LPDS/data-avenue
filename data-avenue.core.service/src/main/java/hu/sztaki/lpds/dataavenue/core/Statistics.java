package hu.sztaki.lpds.dataavenue.core;

import java.util.concurrent.atomic.AtomicLong;

//import javax.persistence.EntityManager;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/*
 * NOTE: the total bytes transferred is not tracked anymore in database
 */
public class Statistics {
//	private static final Logger log = LoggerFactory.getLogger(Statistics.class);
	
	private static long bytesTransferredAtStartup = 0l; 
	private static AtomicLong bytesTransferredSinceStartup = new AtomicLong(0l);

	public static void incBytesTransferred (Long bytes) { // should be invoked by all adaptors
		if (bytes == null || bytes == 0l) return;
		bytesTransferredSinceStartup.addAndGet(bytes);
	}
	
	public static long getBytesTransferred() {
		return bytesTransferredAtStartup + bytesTransferredSinceStartup.get();
	}

	public static void init() { // invoked on stratup
		return;
//		EntityManager entityManager = DBManager.getInstance().getEntityManager();
//		if (entityManager == null) return; 
//		
//		entityManager.getTransaction().begin();
//		BytesTransferred entity = entityManager.find(BytesTransferred.class, BytesTransferred.ROW_ID);
//		if (entity != null) {
//			bytesTransferredAtStartup = entity.getBytesTransferred();
//			log.info("BytesTransferred data read from database");
//		} else {
//			log.info("No previous BytesTransferred data found in database");
//		}
//		entityManager.getTransaction().commit();
//		entityManager.close();		
	}
	
	public static void updateAndResetBytesTransferred() { // invoked on shutdown
		return;
//		EntityManager entityManager = DBManager.getInstance().getEntityManager();
//		if (entityManager == null) return; 
//		entityManager.getTransaction().begin();
//
//		BytesTransferred entity = entityManager.find(BytesTransferred.class, BytesTransferred.ROW_ID);
//		if (entity == null) {
//			entity = new BytesTransferred();
//			long init = bytesTransferredAtStartup; 
//			long delta = bytesTransferredSinceStartup.getAndSet(0l); // delta reset
//			entity.setBytesTransferred(init + delta);
//			entityManager.persist(entity);
//			log.info("New BytesTransferred data stored in database");
//			bytesTransferredAtStartup = init + delta;
//			bytesTransferredSinceStartup.set(0l); 
//		} else {
//			long init = entity.getBytesTransferred(); // current database value
//			long delta = bytesTransferredSinceStartup.getAndSet(0l); // delta reset
//			entity.setBytesTransferred(init + delta); // increment database value (ignore init)
//			bytesTransferredAtStartup = init + delta;
//		}
//		entityManager.getTransaction().commit();
//		entityManager.close();
	}
}
