package hu.sztaki.lpds.dataavenue.core;

import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class HttpAliasRegistry {
	private static final Logger log = LoggerFactory.getLogger(HttpAliasRegistry.class);
	
	static final boolean UPDATE_ALIAS_STATE = false; // transferring, done, etc. (as an alias can be used many times and by many users, it makes no sense) 
	static final int WARNING_ON_NUMBER_OF_ALIASES = 10000;
	private ScheduledExecutorService scheduler = null;
	
	private static final HttpAliasRegistry INSTANCE = new HttpAliasRegistry(); // singleton
	public static HttpAliasRegistry getInstance() { return INSTANCE; }
	private HttpAliasRegistry() {
		try {
			scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.scheduleAtFixedRate(new HttpAliasCleanUpTask(), 0, Configuration.ALIAS_CLEANUP_PERIOD, TimeUnit.SECONDS);
		} catch(Exception e) { log.warn("Cannot schedule transfer clean-up timer!"); }

	}
	
	public void shutdown() {
		log.info("Shutting down alias clean-up timer...");
		if (scheduler != null) scheduler.shutdownNow(); // cleanUpTimer.cancel();
	}
	
	/*
	 * create a HTTP alias entry containing URI, credentials. etc., persist in database, and return its id
	 */
	public String createAndRegisterHttpAlias (final String ticket, final URIBase uri, final Credentials creds, final DataAvenueSession session, final boolean isRead, final int lifetime, final boolean extractArchive, final String directURL) throws NotSupportedProtocolException, URIException, CredentialException, OperationException {
		HttpAlias alias = new HttpAlias(ticket, uri, creds, session, isRead, lifetime, extractArchive, directURL);
		EntityManager entityManager = DBManager.getInstance().getEntityManager();
		if (entityManager != null) {
			entityManager.getTransaction().begin();
			alias.encode(); // encode sensitive credential data
			entityManager.persist(alias);
			entityManager.getTransaction().commit();
			entityManager.close();
		} else throw new OperationException("No database connection");
		return alias.getAliasId();
	}
	
	public HttpAlias getHttpAlias(final String id) throws Exception {
		HttpAlias alias = null; 
		EntityManager entityManager = DBManager.getInstance().getEntityManager();
		if (entityManager != null) {
			entityManager.getTransaction().begin();
			alias = entityManager.find(HttpAlias.class, id);
			entityManager.getTransaction().commit();
			entityManager.close();
		} else log.error("No database connection"); // return null
		if (alias != null) alias.decode(); // decode sensitive credential data
		return alias;
	}

	// rerturns null if alias to be removed not found
	public HttpAlias deleteHttpAlias(final String id) throws Exception {
		HttpAlias alias = null; 
		EntityManager entityManager = DBManager.getInstance().getEntityManager();
		if (entityManager != null) {
			entityManager.getTransaction().begin();
			alias = entityManager.find(HttpAlias.class, id);
			if (alias != null) entityManager.remove(alias);
			entityManager.getTransaction().commit();
			entityManager.close();
			return alias;
		} else throw new Exception("No database connection!"); 
	}
	
	public Collection<HttpAlias> getAllHttpAliases() {
		log.debug("...getting all http aliases from db");
		List<HttpAlias> allAliases = null;
		EntityManager entityManager = DBManager.getInstance().getEntityManager();
		if (entityManager != null) {
			entityManager.getTransaction().begin();
			allAliases = entityManager.createNamedQuery("HttpAlias.findAll", HttpAlias.class ).setMaxResults(100).getResultList();
			entityManager.getTransaction().commit();
			entityManager.close();
		} else log.error("No database connection");
		return allAliases != null ? allAliases : new Vector<HttpAlias>();
	}
	
	public class HttpAliasCleanUpTask extends TimerTask {
		public void run() {
			long now = System.currentTimeMillis();
//			log.trace("Cleaning-up expired aliases... (period: " + Configuration.ALIAS_CLEANUP_PERIOD + "s, now: " + Utils.dateString(now) + ")");
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			if (entityManager != null) {
				entityManager.getTransaction().begin();
				List<HttpAlias> expiredAliases = entityManager.createNamedQuery("HttpAlias.findExpired", HttpAlias.class ).setParameter("p", now).getResultList();
				if (expiredAliases != null && expiredAliases.size() > 0) {
					log.debug("Deleting " + expiredAliases.size() + " rows...");
					for (HttpAlias expired: expiredAliases) entityManager.remove(expired);
				}
				/*int deleted = entityManager.createNamedQuery("HttpAlias.deleteExpired").setParameter("p", now).executeUpdate(); // does not delete alias credentials */ 
				entityManager.getTransaction().commit();
				entityManager.close();
			} else log.error("No database connection");
			
			// NOTE: this should go to another timer task
//			log.trace("Updating bytes transferred data in database..."); 
			Statistics.updateAndResetBytesTransferred(); // update bytes transferred data in database
			
		}
	}
}