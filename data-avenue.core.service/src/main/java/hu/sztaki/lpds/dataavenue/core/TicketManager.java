package hu.sztaki.lpds.dataavenue.core;

import hu.sztaki.lpds.dataavenue.core.Ticket.TicketTypesEnum;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.UnexpectedException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.TicketException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TicketManager {

	private static final Logger log = LoggerFactory.getLogger(TicketManager.class);

	private static final TicketManager INSTANCE = new TicketManager(); // singleton
	public static TicketManager getInstance() { return INSTANCE; }
//	private EntityManager ticketsEm; // dedicated manager for tickets (application-scpe) - IT WOULD NEEED SYNCHRONICATION

	private TicketManager() {
	}

	public void shutdown() {
	}

	public Collection<Ticket> getAllTickets() throws UnexpectedException {
		log.debug("Getting all tickets...");
		EntityManager ticketsEm = DBManager.getInstance().getEntityManager();
		if (ticketsEm != null) {
			ticketsEm.getEntityManagerFactory().getCache().evictAll(); // get a fresh copy of DB
			
			List<Ticket> all = null;
			EntityTransaction transaction = null;
			try {
				transaction = ticketsEm.getTransaction();
				transaction.begin();
				all = ticketsEm.createNamedQuery("Ticket.findAll", Ticket.class).getResultList();
				transaction.commit();
			} catch(Exception e) {
				log.error("Error in transaction: " + e.getMessage(), e);
				if (transaction != null && transaction.isActive()) {
					try { transaction.rollback(); } 
					catch(Exception x) { log.error("Cannot roll back: " + x.getMessage()); }
				}
				throw new UnexpectedException(e);
			}
			finally {
				try { ticketsEm.close(); } 
				catch (IllegalStateException e) { log.error("Cannot close entity manager"); }
			}
			if (all != null) return all;  
		} else log.warn("Ticket entity manager is null");
		return Collections.<Ticket>emptyList();
	}
	
	private Ticket getTicket(final String ticketString) throws TicketException, UnexpectedException {
		if (ticketString == null) throw new TicketException("No key!");
		EntityManager ticketsEm = DBManager.getInstance().getEntityManager();
		if (ticketsEm != null) {
			Ticket ticket = null;
			EntityTransaction transaction = null;
			try {
				transaction = ticketsEm.getTransaction();
				transaction.begin();
				ticket = ticketsEm.find(Ticket.class, ticketString);
				transaction.commit();
			} catch(Exception e) {
				log.error("Error in transaction: " + e.getMessage(), e);
				if (transaction != null && transaction.isActive()) {
					try { transaction.rollback(); } 
					catch(Exception x) { log.error("Cannot roll back: " + x.getMessage()); }
				}
				throw new UnexpectedException(e);
			}
			finally {
				try { ticketsEm.close(); } 
				catch (IllegalStateException e) { log.error("Cannot close entity manager"); }
			}

			if (ticket != null) {
				if (ticket.isDisabled()) throw new TicketException("Access key is inactive");
				long validThru = ticket.getValidThru();
				if (validThru == 0l || validThru >= System.currentTimeMillis()) return ticket; // valid ticket
				else throw new TicketException("Access key has expired!");
			} else {
				throw new TicketException("Access key is (" + ticketString + ") invalid!");
			}
		} else {
			log.error("Ticket entity manager is null");
			throw new TicketException("No database connection. Access key cannot be verified!");
		}
	}
	
	// cache keys 
	Map<String, Long> keyCache = new ConcurrentHashMap <String, Long>(); // TODO on disabling or changing validity should clear key from cache
	// check key validity
	public void verifyKey(final String key) throws TicketException {
		if (key == null) throw new TicketException("Key required!");
		
		// check key cache, and expiration
		Long validThru = keyCache.get(key);
		if (validThru != null) { // cached
			if (validThru == 0l) return; // infinitely valid key
			if (validThru >= System.currentTimeMillis()) return; // valid ticket
			else throw new TicketException("Access key has expired!");
		}
		
		// not cached
		EntityManager ticketsEm = DBManager.getInstance().getEntityManager();
		if (ticketsEm != null) {
			Ticket ticket = null;
			EntityTransaction transaction = null;
			try {
				transaction = ticketsEm.getTransaction();
				transaction.begin();
				ticket = ticketsEm.find(Ticket.class, key);
				transaction.commit();
			} catch(Exception e) {
				log.error("Error in transaction: " + e.getMessage(), e);
				if (transaction != null && transaction.isActive()) {
					try { transaction.rollback(); } 
					catch(Exception x) { log.error("Cannot roll back: " + x.getMessage()); }
				}
				throw new TicketException(e);
			}
			finally {
				try { ticketsEm.close(); } 
				catch (IllegalStateException e) { log.error("Cannot close entity manager"); }
			}

			if (ticket != null) {
				if (ticket.isDisabled()) throw new TicketException("Inactive access key!");
				long validity = ticket.getValidThru();
				keyCache.put(key, validity);
				if (validity == 0l || validity >= System.currentTimeMillis()) return; // valid key
				else throw new TicketException("Access key has expired!");
			} else {
				throw new TicketException("Access key (" + key + ") is invalid!");
			}
		} else {
			log.error("Entity manager is null");
			throw new TicketException("No database connection. Access key cannot be verified!");
		}

	}
	
	public void checkTicket(final String ticket) throws TicketException, UnexpectedException {
		getTicket(ticket);
	}
	
	public String createUserTicket(final String adminTicketString, final String name, final String company, final String email) throws TicketException, OperationException {
		Ticket adminTicket = getTicket(adminTicketString);
		if (adminTicket.getTicketType() != TicketTypesEnum.PORTAL_ADMIN  && adminTicket.getTicketType() != TicketTypesEnum.ADMIN) throw new TicketException("Ticket is not a protal admin ticket!");

		Ticket userTicket = new Ticket();
		if (email == null) throw new OperationException("E-mail address required for creating a new ticket!");
		userTicket.setEmail(stringOfMaxSize(email, Ticket.STRING_SQL_SIZE));
		if (name != null) userTicket.setName(stringOfMaxSize(name, Ticket.STRING_SQL_SIZE));
		else userTicket.setName("Generated (Noname)");
		if (company != null) userTicket.setCompany(stringOfMaxSize(company, Ticket.STRING_SQL_SIZE));
		else userTicket.setCompany(adminTicket.getCompany());
		userTicket.setParent(adminTicket.getTicket()); // set creator
		userTicket.setValidThru(adminTicket.getValidThru()); // inherit limits
		userTicket.setMaxAliases(adminTicket.getMaxAliases());
		userTicket.setMaxTransfers(adminTicket.getMaxTransfers());
		userTicket.setMaxSessions(adminTicket.getMaxSessions());
		userTicket.setTicketType(TicketTypesEnum.PORTAL_USER);
		
		userTicket.setComments("Generated (PORTAL_USER)");
		userTicket.setLatitude(adminTicket.getLatitude());
		userTicket.setLongitude(adminTicket.getLongitude());
		userTicket.setAccuracy(adminTicket.getAccuracy());
		
		EntityManager ticketsEm = DBManager.getInstance().getEntityManager();
		if (ticketsEm != null) {
			EntityTransaction transaction = null;
			try {
				transaction = ticketsEm.getTransaction();
				transaction.begin();
				ticketsEm.persist(userTicket);
				ticketsEm.flush();
				transaction.commit();
			} catch(Exception e) {
				log.error("Error in transaction: " + e.getMessage(), e);
				if(transaction != null && transaction.isActive()) {
					try { transaction.rollback(); } 
					catch(Exception x) { log.error("Cannot roll back: " + x.getMessage()); }
				}
				throw new UnexpectedException(e);
			}
			finally {
				try { ticketsEm.close(); } 
				catch (IllegalStateException e) { log.error("Cannot close entity manager"); }
			}
		}

		return userTicket.getTicket();
	}
	
	public static void createKey(final String id, final String type, final String name, final String company, final String email) throws TicketException, OperationException {
		log.debug("Creating key: " + id);
		EntityManager ticketsEm = DBManager.getInstance().getEntityManager();
		if (ticketsEm != null) {
			
			Ticket ticket = null;
			EntityTransaction transaction = null;
			try {
				transaction = ticketsEm.getTransaction();
				transaction.begin();
				ticket = ticketsEm.find(Ticket.class, id);
				if (ticket == null) { // new id
					log.info("Adding key to database");
					Ticket userTicket = new Ticket();
					userTicket.setTicket(id);
					userTicket.setEmail(stringOfMaxSize(email, Ticket.STRING_SQL_SIZE));
					userTicket.setName(stringOfMaxSize(name, Ticket.STRING_SQL_SIZE));
					userTicket.setCompany(stringOfMaxSize(company, Ticket.STRING_SQL_SIZE));
					userTicket.setValidThru(0l); // forever
					userTicket.setMaxAliases(0); // unlimited
					userTicket.setMaxTransfers(0);
					userTicket.setMaxSessions(0);
					userTicket.setTicketType("api".equals(type) ? TicketTypesEnum.API_USER : TicketTypesEnum.PORTAL_ADMIN);
					userTicket.setComments("");
					userTicket.setLatitude(0f);
					userTicket.setLongitude(0f);
					userTicket.setAccuracy(0f);
					ticketsEm.persist(userTicket);
					ticketsEm.flush();
				} else {
					log.debug("Key already exists");
				}
				transaction.commit();
			} catch(Exception e) {
				log.error("Error in transaction: " + e.getMessage(), e);
				if (transaction != null && transaction.isActive()) {
					try { transaction.rollback(); } 
					catch(Exception x) { log.error("Cannot roll back: " + x.getMessage()); }
				}
			}
			finally {
				try { ticketsEm.close(); } 
				catch (IllegalStateException e) { log.error("Cannot close entity manager"); }
			}
		} 
	}
	
	private static String stringOfMaxSize(String s, int size) {
		return s.substring(0, size < s.length() ? size : s.length());
	}
	
	public void newSession(final String ticketString) throws TicketException, UnexpectedException {
		Ticket ticket = getTicket(ticketString);
		if (ticket.getMaxSessions() > 0 && ticket.activeSessions.incrementAndGet() > ticket.getMaxSessions()) { // don't count sessions if no limit 
			log.warn("Ticket sessions exceeded the limit!"); // throw exception on production
		}
		if (ticket.getMaxSessions() > 0) log.debug("Sessions per ticket: " + ticket.activeSessions.get());
	}
	
	public void sessionDestroyed(final String ticketString) throws TicketException, UnexpectedException {
		Ticket ticket = getTicket(ticketString);
		if (ticket.getMaxSessions() > 0) {
			ticket.activeSessions.decrementAndGet();
			log.debug("Sessions per ticket: " + ticket.activeSessions.get());
		}		
	}
}