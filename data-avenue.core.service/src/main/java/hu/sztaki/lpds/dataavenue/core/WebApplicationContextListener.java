package hu.sztaki.lpds.dataavenue.core;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebApplicationContextListener implements ServletContextListener {
	private static final Logger log = LoggerFactory.getLogger(WebApplicationContextListener.class);

	public void contextInitialized(ServletContextEvent sce) {
		log.info("===================================================================");
		log.info("STARTING UP WEB APPLICATION...");
		Statistics.init();
//		Aliases are not supported anymore
//		HttpAliasRegistry.getInstance(); // to start-up alias cleanup timer
		TaskManager.getInstance(); // to start-up task cleanup timer
		Heartbeat.getInstance();
		
		try {
			// needed to avoid conflict with UriBuilder abstract method jersey 1.19 (hdfs adaptor dependency) vs. 2.17 (all the others)
			javax.ws.rs.ext.RuntimeDelegate.setInstance(new org.glassfish.jersey.internal.RuntimeDelegateImpl());
			log.info("Jersey 2.x RuntimeDelegate registered");
		} catch (Throwable x) {
			log.error("Cannot set JAX-RS 2.x org.glassfish.jersey.internal.RuntimeDelegateImpl (check jar: WEB-INF/lib/jersey-server-2.17 jar)", x);
		}
	}

	public void contextDestroyed(ServletContextEvent sce) {
		log.info("SHUTTING DOWN WEB APPLICATION...");
		log.info("===================================================================");
		Statistics.updateAndResetBytesTransferred();
		TicketManager.getInstance().shutdown();
		DBManager.getInstance().shutdown();
		TaskManager.getInstance().shutdown();
//		HttpAliasRegistry.getInstance().shutdown();
    	CopyTaskManager.getInstance().shutdown();
    	Heartbeat.getInstance().shutdown();
    	System.runFinalization();
    	System.gc(); // avoids some memory leaks
    	log.info("Bye!");
		//org.apache.commons.logging.LogFactory.releaseAll(); ??? FIXME
	}
}