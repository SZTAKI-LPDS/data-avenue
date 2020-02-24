package hu.sztaki.lpds.dataavenue.core;

import hu.sztaki.lpds.dataavenue.interfaces.Limits;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import org.ogf.saga.error.IncorrectStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.in2p3.jsaga.adaptor.base.defaults.EnvironmentVariables;

public class Configuration {
	private static final Logger log = LoggerFactory.getLogger(Configuration.class);
	
	// configuration properties file name
	public final static String PROPERTIES_FILE_NAME = "dataavenue.properties";

	private static String version = "3.0"; // initial, default version (not used unless automatic version detection fails)

	public static String tempDir = null;
	public static String certificatesDirectory = null;
	public static String vomsDirectory = null;
	
	// DataAvenue accepts sync commands such as list, mkdir, del 
	public static volatile boolean acceptCommands = true;
	public static void setAcceptCommands(final boolean value) { acceptCommands = value; log.info("acceptCommands: " + acceptCommands);}
	public static boolean getAcceptCommands() { return acceptCommands; }

	// DataAvenue accepts async commands such as copy/move
	public static volatile boolean acceptCopyCommands = true;
	public static void setAcceptCopyCommands(final boolean value) { acceptCopyCommands = value;	log.info("acceptCopyCommands: " + acceptCopyCommands); }
	public static boolean getAcceptCopyCommands() {	return acceptCopyCommands; }
	
	// DataAvenue accepts requests via aliases (get/put)
	public static volatile boolean acceptHttpAliases = true;
	public static void setAcceptHttpAliases(final boolean value) { acceptHttpAliases = value; log.info("acceptHttpAliases: " + acceptHttpAliases); }
	public static boolean getAcceptHttpAliases() { return acceptHttpAliases; }

	// maximum number of allowed parallel active downloads via aliases  
	public static volatile int maxNumberOfParallelDownloads = 100;
	
	// maximum number of allowed parallel active uploads via aliases
	public static volatile int maxNumberOfParallelUploads = 100;
	
	// transfers...
	static final int MAXIMUM_NUMBER_OF_ACTIVE_TRANSFER_THREADS = 100; // total per server used in CopyTaskManager
	static int TRANSFER_EXPIRATION_TIME = 30 * 86400; // configurable, transfer details removed from DB after default: 30 days (in secs)
	static final int TRANSFER_CLEANUP_PERIOD =  24 * 60 * 60; // expired aliases checked and removed: every day (in secs)

	// aliases...
	public static final boolean CHECK_ALIASES_ON_CREATION = false; // file read/created on get creation if alias is created for reading/writing  
	static final int DEFAULT_ALIAS_EXPIRATION_TIME = 86400; // aliases default lifetime (if not specified): 1 day (in secs)
	static final int ALIAS_CLEANUP_PERIOD = 60 * 60; // expired aliases checked and removed every hour (in secs)
	
	private static volatile String aliasCredentialsEncriptionKey = "LesPassword"; // mot de passe
	public static String getAliasCredentialsEncriptionKey() { return aliasCredentialsEncriptionKey; }
	public static void setAliasCredentialsEncriptionKey( String aliasCredentialsEncriptionKey) { Configuration.aliasCredentialsEncriptionKey = aliasCredentialsEncriptionKey; }

	public static volatile String initialAdminTicket = null;

	public static String privateIP = "127.0.0.1";
	public static String instanceId = UUID.randomUUID().toString();
	
	// see Heartbeat.java
	public static long HEARTBEAT_FREQUENCY = 0; // in seconds, send hearbeat to database, 0 means dont send
	public static long INSTANCE_DEAD_THRESHOLD = 0; // in seconds, hearbeat older than this value makes instance dead, 0: never dead
	public static long DEAD_INSTANCE_CLEANUP_FREQUENCY = 0; // in seconds, check if cleanup needed, 0 means dont do it
	public static long DEAD_INSTANCE_CLEANUP_THRESHOLD = 0; // in seconds, heartbeat older than this value cleans up its ongoing (abondoned) transfers, 0: dont clean up
	
	// read from properties file
	static {

		try { tempDir = System.getProperty("java.io.tmpdir"); } 
		catch (Throwable e) { log.error("Cannot read system property: java.io.tmpdir"); }
		if (tempDir == null) tempDir = "./";
		if (!new File(tempDir).exists()) log.error("Cannot access temp directory: " + tempDir);
		else log.info("Temp dir: " + tempDir);
		
		// try to read properties file
		Properties prop = new Properties();
		log.info("Trying to read configuration file: " + PROPERTIES_FILE_NAME + " (WEB-INF/classes, WEB-INF/lib/*.jar)");
		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME); // emits no exception but null returned
		if (in == null) { log.warn("The system cannot find the file specified. Using defaults."); } 
		else {
			try {
				prop.load(in);
				try { in.close(); } catch (IOException e) {}
		
				if (prop.get("version") != null) version = (String) prop.get("version");
		
				if ("disabled".equals(prop.get("acceptCommands"))) acceptCommands = false; // else leave default true
				if ("disabled".equals(prop.get("acceptCopyCommands"))) acceptCopyCommands = false; // else leave default true
				if ("disabled".equals(prop.get("acceptHttpAliases"))) acceptHttpAliases = false; // else leave default true
			
				try { 
					int value = Integer.parseInt((String) prop.get("maxNumberOfParallelDownloads"));
					if (value >= 0) maxNumberOfParallelDownloads = value; // else leave default 100
				} catch (NumberFormatException e) {} // missing or invalid
		
				try { 
					int value = Integer.parseInt((String) prop.get("maxNumberOfParallelUploads"));
					if (value >= 0) maxNumberOfParallelUploads = value; // else leave default 100
				} catch (NumberFormatException e) {} // missing or invalid
					
				try { 
					int value = Integer.parseInt((String) prop.get("transferDetailsTimeout"));
					if (value >= 0) TRANSFER_EXPIRATION_TIME = value; // else leave default
				} catch (NumberFormatException e) {} // missing or invalid
			
				if (prop.get("certificatesDirectory") != null) { // property file takes precedence
					certificatesDirectory = (String) prop.get("certificatesDirectory");
				}

				if (prop.get("initialAdminTicket") != null) { // property file takes precedence
					initialAdminTicket = (String) prop.get("initialAdminTicket");
				}
				
				try { 
					int value = Integer.parseInt((String) prop.get("maxErrors"));
					if (value >= 0) Limits.MAX_ERRORS = value; // else leave default
				} catch (NumberFormatException e) {} // missing or invalid
				try { 
					long value = Long.parseLong((String) prop.get("maxBytesToRetransferr"));
					if (value >= 0) Limits.MAX_BYTES_TO_RETRANSFER = value; // else leave default
				} catch (NumberFormatException e) {} // missing or invalid
				try { 
					long value = Long.parseLong((String) prop.get("errorRetryDelay"));
					if (value >= 0) Limits.ERROR_RETRY_DELAY = value; // else leave default
				} catch (NumberFormatException e) {} // missing or invalid

				try { 
					int value = Integer.parseInt((String) prop.get("heartbeatFrequency"));
					if (value >= 0) HEARTBEAT_FREQUENCY = value;
				} catch (NumberFormatException e) {
					if (prop.get("heartbeatFrequency") != null) log.warn("heartbeatFrequency parse exception", e);
				} 

				try { 
					int value = Integer.parseInt((String) prop.get("instanceDeadThreshold"));
					if (value >= 0) INSTANCE_DEAD_THRESHOLD = value;
				} catch (NumberFormatException e) {
					if (prop.get("instanceDeadThreshold") != null) log.warn("instanceDeadThreshold parse exception", e);
				} 

				try { 
					int value = Integer.parseInt((String) prop.get("deadInstanceCleanUpFrequency"));
					if (value >= 0) DEAD_INSTANCE_CLEANUP_FREQUENCY = value;
				} catch (NumberFormatException e) {
					if (prop.get("deadInstanceCleanUpFrequency") != null) log.warn("deadInstanceCleanUpFrequency parse exception", e);
				} 

				try { 
					int value = Integer.parseInt((String) prop.get("deadInstanceCleanUpThreshold"));
					if (value >= 0) DEAD_INSTANCE_CLEANUP_THRESHOLD = value;
				} catch (NumberFormatException e) {
					if (prop.get("deadInstanceCleanUpThreshold") != null) log.warn("deadInstanceCleanUpThreshold parse exception", e);
				} 
				
				log.info("Limits.MAX_ERRORS: " + Limits.MAX_ERRORS);
				log.info("Limits.MAX_BYTES_TO_RETRANSFERR (bytes): " + Limits.MAX_BYTES_TO_RETRANSFER);
				log.info("Limits.ERROR_RETRY_DELAY (ms): " + Limits.ERROR_RETRY_DELAY);

				log.info("Heartbeat settings:");
				log.info("  HEARTBEAT_FREQUENCY: " + HEARTBEAT_FREQUENCY + "s");
				log.info("  INSTANCE_DEAD_THRESHOLD: " + INSTANCE_DEAD_THRESHOLD + "s");
				log.info("  DEAD_INSTANCE_CLEANUP_FREQUENCY: " + DEAD_INSTANCE_CLEANUP_FREQUENCY + "s");
				log.info("  DEAD_INSTANCE_CLEANUP_THRESHOLD: " + DEAD_INSTANCE_CLEANUP_THRESHOLD + "s");
				
				log.info("Configuration loaded");
				
			} catch (IOException e) { log.error("Cannot read the configuration file", e); } // prop.load(in);
		} // end if (in == null) else

		if (prop.get("vomsDirectory") != null) { // property file takes precedence
			vomsDirectory = (String) prop.get("vomsDirectory");
		}
		
		if (vomsDirectory == null) {
			String candidate = "/etc/grid-security/vomsdir";
			log.info("Checking VOMS directory: " + candidate);
			try { if (new File(candidate).isDirectory()) vomsDirectory = candidate; } catch (SecurityException e) {}
		}
		
		if (vomsDirectory == null) { 
			try {
				String candidate = System.getProperty("user.home") + "/.globus/vomsdir"; ;
				log.info("Checking VOMS directory: " + candidate);
				if (new File(candidate).isDirectory()) vomsDirectory = candidate;  
			} catch (SecurityException e) {}
		}
		
		if (vomsDirectory == null) { // if not found, create voms dir in user's home
			String candidate = System.getProperty("user.home") + "/.globus/vomsdir";
			log.warn("No VOMS directory! Creating: " + candidate);
			try { new File(candidate).mkdirs(); } catch(Exception e) { log.error("Cannot create dir: " + candidate); }
			vomsDirectory = candidate;
		} 
		
		log.info("VOMS dir: " + vomsDirectory);
		
		if (certificatesDirectory == null) { // property file takes precedence, set if not yet set
				
			String candidate; 
			
			EnvironmentVariables env = null; // read what jSaga thinks
			try { env = EnvironmentVariables.getInstance();	} catch (IncorrectStateException e) {}
			
			if (certificatesDirectory == null && env != null) { 
				log.info("Checking certificates directory: ${CADIR} (environment variable)");
				candidate = env.getProperty("CADIR");
				if (candidate != null) {
					try { if (new File(candidate).isDirectory()) certificatesDirectory = candidate; } catch (SecurityException e) {}
				}
			}
			
			if (certificatesDirectory == null && env != null) { 
				log.info("Checking certificates directory: ${X509_CERT_DIR} (environment variable)");
				candidate = env.getProperty("X509_CERT_DIR");
				if (candidate != null) {
					try { if (new File(candidate).isDirectory()) certificatesDirectory = candidate; } catch (SecurityException e) {}
				}
			}
			
			if (certificatesDirectory == null) {
				log.info("Checking certificates directory: /etc/grid-security/certificates");
				candidate = "/etc/grid-security/certificates";
				if (candidate != null) {
					try { if (new File(candidate).isDirectory()) certificatesDirectory = candidate; } catch (SecurityException e) {}
				}
			}

			if (certificatesDirectory == null) { 
				try {
					log.info("Checking certificates directory ${user.home} (system property): " + System.getProperty("user.home") + "/.globus/certificates/");
					candidate = System.getProperty("user.home");
					if (candidate != null) candidate += "/.globus/certificates/"; 
					try { if (new File(candidate).isDirectory()) certificatesDirectory = candidate; } 
					catch (SecurityException e) {}
				} catch (SecurityException e) {}
			}
				
			if (certificatesDirectory == null) {
				log.warn("No standard location of certificates directory found! Setting certificates directory to ./certificates");
				certificatesDirectory = "./certificates";
			} else {
				log.info("Certificates directory: " + certificatesDirectory + "");
			}
		}

		// check DB connectiviy
		DBManager dbManager = DBManager.getInstance();

		if (initialAdminTicket != null) {
			log.info("Found initial admin ticket in " + PROPERTIES_FILE_NAME);
			dbManager.registerInitialAdminAccessKey(initialAdminTicket);
		}
		try { privateIP =  java.net.InetAddress.getLocalHost().getHostAddress(); } // FIXME skip loopback IPs 
		catch(Exception x) { log.warn("Cannot get local IP"); } 
		log.info("Local IP: " + privateIP);

	} // end of static
	
	public static String getVersion() {
		return version;  
	}	
}
