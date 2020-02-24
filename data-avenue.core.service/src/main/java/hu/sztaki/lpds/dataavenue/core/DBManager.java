package hu.sztaki.lpds.dataavenue.core;

import hu.sztaki.lpds.dataavenue.core.Ticket.TicketTypesEnum;

import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBManager {
	
	private static final Logger log = LoggerFactory.getLogger(DBManager.class);

	private static final String PERSISTENCE_UNIT_NAME = "hu.sztaki.lpds.dataavenue.jpa";
	private EntityManagerFactory entityManagerFactory = null;
	public final static int SQL_DEFAULT_STRING_LENGTH = 255; // FIXME check all persisted String fields!!!
	
	private static int dbConnectionFailures = 0;
	private static final int MAX_DB_CONNECTION_FAILURES = 100;
	
	private static final DBManager INSTANCE = new DBManager(); // singleton
	public static DBManager getInstance() { 
		return INSTANCE; 
	}
	
	private synchronized void establishDBConnection() {
		if (dbConnectionFailures >= MAX_DB_CONNECTION_FAILURES) { 
			if (dbConnectionFailures == MAX_DB_CONNECTION_FAILURES) {
				log.error("No more trials to connect to DB. Max trials reached: " + MAX_DB_CONNECTION_FAILURES);
				dbConnectionFailures++;
			}
			return;
		}
		log.debug("Trying to establish database connection...");
		try {
			EntityManagerFactory emf;
			final String DATAAVENUE_DATABASE = "DATAAVENUE_DATABASE";
			final String DATABASE_NAME = "dataavenue";
			if (System.getenv(DATAAVENUE_DATABASE) != null) {
				String connectionUrl = "jdbc:mysql://" + System.getenv(DATAAVENUE_DATABASE) + "/" + DATABASE_NAME;
				log.info("Environment variable " + DATAAVENUE_DATABASE + " is set to: " + System.getenv(DATAAVENUE_DATABASE));
				log.info("Using hibernate.connection.url: " + connectionUrl);
				Properties props = new Properties();
				props.setProperty("hibernate.connection.url", connectionUrl);
				emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT_NAME, props);
			} else {
				log.info("Using default hibernate.connection.url in META-INF/persistence.xml");
				emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT_NAME);
			}
			
			log.debug("emf.getProperties(): " + emf.getProperties());

			log.debug("Creating entity manager...");
			EntityManager em = emf.createEntityManager();
			log.debug("Testing transaction...");
			em.getTransaction().begin(); // test connection
			em.getTransaction().commit();
			em.close();
			log.info("Database connection established!");
			this.entityManagerFactory = emf;
		} catch (Throwable e) {
			org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ALL);
			log.error("Could not open database connection (" + PERSISTENCE_UNIT_NAME + "): " + (entityManagerFactory != null ? entityManagerFactory.getProperties() : "(entityManagerFactory is null)"), e);
			dbConnectionFailures++;
		}
		registerInitialAdminAccessKey(null);
	}
	
	private DBManager() {
		establishDBConnection();
	}

	void shutdown() {
		if (entityManagerFactory != null) { // close entity manager factory
			log.debug("Closing entity manager factory...");
			try { entityManagerFactory.close();	} 
			catch (Throwable e) { log.warn("Cannot close entity manager factory!", e); }
		}
	}
	
	EntityManager getEntityManager() {
		if (entityManagerFactory == null) {
			establishDBConnection();
			if (entityManagerFactory == null) {
				log.error("No database connection!"); 
				return null;
			}
		}
		EntityManager em = null;
		try {
			em = entityManagerFactory.createEntityManager();
		} catch (IllegalStateException e) {
			log.error("Cannot create entity manager!", e);
		}
		return em;
	}
	
	private String initialAdminAccessKey = null;
	private boolean initialAdminAccessKeyRegistered = false;
	
	public void registerInitialAdminAccessKey(String key) {
		if (this.initialAdminAccessKeyRegistered) return;
		if (key != null) this.initialAdminAccessKey = key;
		if (this.initialAdminAccessKey == null) return;
		
		EntityManager ticketsEm = DBManager.getInstance().getEntityManager();
		if (ticketsEm != null) {
			Ticket ticket = null;
			EntityTransaction transaction = null;
			try {
				transaction = ticketsEm.getTransaction();
				transaction.begin();
				ticket = ticketsEm.find(Ticket.class, key);
				if (ticket == null) {
					log.info("Adding initial admin ticket to database...");
					Ticket userTicket = new Ticket();
					userTicket.setTicket(key);
					userTicket.setEmail("admin@dataavenue.com");
					userTicket.setName("Initial Admin");
					userTicket.setCompany(null);
					userTicket.setValidThru(0l); // forever
					userTicket.setMaxAliases(0);
					userTicket.setMaxTransfers(0);
					userTicket.setMaxSessions(0);
					userTicket.setTicketType(TicketTypesEnum.PORTAL_ADMIN);
					userTicket.setComments("Initial PORTAL_ADMIN ticket created automatically");
					userTicket.setLatitude(0f);
					userTicket.setLongitude(0f);
					userTicket.setAccuracy(0f);
					ticketsEm.persist(userTicket);
					ticketsEm.flush();
				} else {
					log.debug("Initial admin ticket already exists");
				}
				transaction.commit();
				log.info("Initial admin ticket registered...");
				initialAdminAccessKeyRegistered = true;
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
	
	public static String abbreviate(String str) {
		if (str == null) return null;
		if (str.length() <= SQL_DEFAULT_STRING_LENGTH) return str;
		else return str.substring(0, SQL_DEFAULT_STRING_LENGTH - 3) + "...";
	}
}