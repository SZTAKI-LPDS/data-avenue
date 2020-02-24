package hu.sztaki.lpds.dataavenue.core;

import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.GSIFTP_PROTOCOL;
import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.HTTPS_PROTOCOL;
import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.HTTP_PROTOCOL;
import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.IRODS_PROTOCOL;
import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.LFC_PROTOCOL;
import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.SFTP_PROTOCOL;
import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.SRM_PROTOCOL;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Akos Hajnal
 *
 * Ftp see: http://maven.apache.org/wagon/
 */
public final class AdaptorRegistry {
	
	private static final Logger log = LoggerFactory.getLogger(AdaptorRegistry.class);
	private static final String DATA_AVENUE_ADAPTOR_PROPERTIES = "META-INF/data-avenue-adaptor.properties";
	private static final String DATA_AVENUE_ADAPTOR_CLASS_NAME_PROPERTY = "adaptorClassName";

	private AdaptorRegistry () { throw new AssertionError(); } // avoid instantiation

	private static Map<String, Adaptor> adaptors; // done in static {} = new ConcurrentHashMap<String, Adaptor<?,?,?>> ();

	static {
		// TODO: first register "custom" adaptors, then built-in (jSAGA) adaptors
		
		// create adaptor registry
		adaptors = new ConcurrentHashMap<String, Adaptor> ();
		
		// register adaptors
		// reflections does not work to search for classes in jars!
		/*Reflections reflections = new Reflections("");
		Set<Class<? extends Adaptor>> adaptorCandidates = reflections.getSubTypesOf(Adaptor.class);
		for (Class<? extends Adaptor> adaptorClass: adaptorCandidates) {
			Adaptor adaptorInstance = newAdaptorInstance(adaptorClass.getName());
			if (adaptorInstance != null) 
				for (String protocol: adaptorInstance.getSupportedProtocols()) 
					registerAdaptor(protocol, adaptorInstance);
		}
		reflections = null;
		System.gc();*/
		
		/*String jSagaGenericAdaptorClassName = "hu.sztaki.lpds.dataavenue.adaptors.jsaga.JSagaGenericAdaptor"; 
		Adaptor jSagaGenericAdaptorInstance = newAdaptorInstance(jSagaGenericAdaptorClassName);
		if (jSagaGenericAdaptorInstance != null) {
			List <String> protocols = jSagaGenericAdaptorInstance.getSupportedProtocols();
			if (protocols != null) for (String protocol: protocols) 
				registerAdaptor(protocol, jSagaGenericAdaptorInstance);
		}*/

		List<String> adaptorClassNames = scanDataAvenueAdaptors();
		for (String adaptorClassName: adaptorClassNames) {
			Adaptor adaptorInstance = newAdaptorInstance(adaptorClassName);
			if (adaptorInstance != null) {
				List <String> protocols = adaptorInstance.getSupportedProtocols();
				if (protocols != null) for (String protocol: protocols) 
					registerAdaptor(protocol, adaptorInstance);
			}
		}
		
		log.info("DataAvenue: " + adaptors.size() + " protocols registered");
	}
	
	private static List<String> scanDataAvenueAdaptors() {
        List<String> result = new ArrayList<String>();
        try {
        	log.info("Searching for adaptor properties files: " + DATA_AVENUE_ADAPTOR_PROPERTIES + "...");
            Enumeration<URL> e = AdaptorRegistry.class.getClassLoader().getResources(DATA_AVENUE_ADAPTOR_PROPERTIES);
            if (!e.hasMoreElements()) log.warn("No adaptor properties files found!");
            while (e.hasMoreElements()) {
                URL url = (URL) e.nextElement();
                log.info("Found properties file: " + url);
                
                Properties prop = new Properties();
                InputStream pis = url.openStream();
                prop.load(pis);
                pis.close();
                
                Object adaptorClassNameProp = prop.get(DATA_AVENUE_ADAPTOR_CLASS_NAME_PROPERTY);
                if (adaptorClassNameProp == null) {
                	log.warn("Property " + DATA_AVENUE_ADAPTOR_CLASS_NAME_PROPERTY + " is missing from properties file!");
                	continue;
                }
                result.add((String) adaptorClassNameProp);
                log.info("Adaptor class: " + adaptorClassNameProp);
            }
        } catch(Exception e) {
            log.error("Exception at adaptor properties files scanning!", e);
        }
        return result;
	}
	
	
	private static void registerAdaptor(final String protocol, final Adaptor adaptorInstance) {
		
		if (protocol == null || adaptorInstance == null) throw new NullPointerException("Parameter value null");
		
		if (adaptors.containsKey(protocol)) {
			log.warn("Adaptor has already been registered for protocol: '" + protocol + "' (" + adaptorInstance.getClass().getName() + " ignored)");
			return;
		}
		
		adaptors.put(protocol, adaptorInstance);
		log.debug("'" + protocol + "://' adaptor registered (" + adaptorInstance.getClass().getName() + ")");
	}

	private static Adaptor newAdaptorInstance(final String adaptorClassName) {
		
		// get class of adaptorClassName
		Class<?> adaptorClass;
		try {
			adaptorClass = Class.forName(adaptorClassName);
		} catch (ClassNotFoundException e) {
			log.warn("Could not load adaptor class: " + adaptorClassName + " (" + e.getMessage() + ")");
			return null;
		} catch (ExceptionInInitializerError e) {
			log.warn("Exception occued during static initializer of class: " + adaptorClassName + " (" + e.getMessage() + ")");
			return null;
		} catch (LinkageError e) {
			log.warn("Cannot load dependencies of class: " + adaptorClassName + " (" + e.getMessage() + ")");
			return null;
		}
		
		// get public constructor
		Constructor<?> adaptorConstructor;
		try {
			adaptorConstructor = adaptorClass.getConstructor();
		} catch (SecurityException x) {
			log.warn("Cannot instantiate class: " + adaptorClass.getName() + " (missing public constructor)");
			return null;
		} catch (NoSuchMethodException x) {
			log.warn("Cannot instantiate class: " + adaptorClass.getName() + " (missing parameterless constructor)");
			return null;
		}
		
		// invoke public constructor
		Object instance;
		try { 
			instance = adaptorConstructor.newInstance();
		} catch (IllegalArgumentException x) {
			log.warn("Cannot instantiate class: " + adaptorClass.getName() + " (missing parameterless constructor)");
			return null; 
		} catch (InstantiationException x) {
			log.warn("Cannot instantiate class: " + adaptorClass.getName() + " (class not instantiatable)");
			return null; 
		} catch (IllegalAccessException x) {
			log.warn("Cannot instantiate class: " + adaptorClass.getName() + " (missing public constructor)");
			return null; 
		} catch (InvocationTargetException x) {
			log.warn("Cannot instantiate class: " + adaptorClass.getName() + " (constructor throws exception)", x);
			return null; 
		}
		
		// check its Adaptor interface
		if (!(instance instanceof Adaptor)) {
			log.warn("Adaptor class " + adaptorClass.getName() + " does not implement the Adaptor interface");
			return null;
		}
		
		// cast and return adaptor instance
		Adaptor adaptorInstance = (Adaptor) instance;
		log.debug("New instance of adaptor " + adaptorClass.getName() + " has been successfuly created");
		return adaptorInstance;
	}
	
	public static Adaptor getAdaptorInstance(final String protocol) throws NotSupportedProtocolException { 
		
		if (protocol == null) throw new NotSupportedProtocolException("No protocol specified!");
		
		if (!adaptors.containsKey(protocol)) throw new NotSupportedProtocolException("No adaptor registered for protocol: " + protocol); // return null
		else return adaptors.get(protocol);
	}
	
	
	public static void print() {
		log.debug("List of adaptors (" + adaptors.size() + "):");
		for (String protocol: adaptors.keySet()) 
			log.debug("protocol: '" + protocol + "://', adaptor: " + adaptors.get(protocol).getClass().getName());
	}
	
	@SuppressWarnings("deprecation")
	public static List<String[]> getAdaptorDetails() { 
		List<String[]> list = new ArrayList<String[]> ();
		
		// sorted by name, treemap
		Map<String, String[]> protocols = new TreeMap<String, String[]>();
		for (String protocol: adaptors.keySet()) {
			String desc[] = new String [5];
			desc[0] = protocol;
			Adaptor a = adaptors.get(protocol);
			desc[1] = a.getName();
			desc[2] = a.getDescription();
			desc[3] = a.getAuthenticationTypes(protocol).toString();
			desc[4] = a.getClass().getName();
			protocols.put(protocol, desc);
		}

		for (Map.Entry<String, String[]> pair: protocols.entrySet()) 
			list.add(pair.getValue());

		return list;
	}
	
	private static List<String> orderProtocols(List<String> unorderedProtocolList) {
        // order protocols by frequency...
        List<String> orderedProtocolList = new ArrayList<String>();
        if (unorderedProtocolList.remove(HTTP_PROTOCOL)) orderedProtocolList.add(HTTP_PROTOCOL);
    	if (unorderedProtocolList.remove(HTTPS_PROTOCOL)) orderedProtocolList.add(HTTPS_PROTOCOL);
    	if (unorderedProtocolList.remove(SFTP_PROTOCOL)) orderedProtocolList.add(SFTP_PROTOCOL);
    	if (unorderedProtocolList.remove(GSIFTP_PROTOCOL)) orderedProtocolList.add(GSIFTP_PROTOCOL);
    	if (unorderedProtocolList.remove(SRM_PROTOCOL)) orderedProtocolList.add(SRM_PROTOCOL);
    	if (unorderedProtocolList.remove(LFC_PROTOCOL)) orderedProtocolList.add(LFC_PROTOCOL);
    	if (unorderedProtocolList.remove(IRODS_PROTOCOL)) orderedProtocolList.add(IRODS_PROTOCOL);
    	orderedProtocolList.addAll(unorderedProtocolList); // add the rest
    	return orderedProtocolList;
	}
	
	public static List<String> getSupportedProtocols() {
		List<String> result = new ArrayList<String>();
		result.addAll(adaptors.keySet());
		return orderProtocols(result);
	}
	
	// protocol -> (auth type -> usage)
	@SuppressWarnings("deprecation")
	public static Map<String, List<String[]>> getProtocolAuthenticationDetails() {
		Map<String, List<String[]>> result = new TreeMap<String, List<String[]>>(); 
		for (String protocol: adaptors.keySet()) {
			Adaptor a = adaptors.get(protocol);
			List<String[]> authTypeAndUsageList = new ArrayList<String[]>();
			for (String type: a.getAuthenticationTypes(protocol)) {
				String [] auth = new String[2];
				auth[0] = type;
				auth[1] = a.getAuthenticationTypeUsage(protocol, type);
				authTypeAndUsageList.add(auth);
			}
			result.put(protocol, authTypeAndUsageList);
		}
		return result;
	}
	
	public static List<Adaptor> getAdaptors() { 
		List<Adaptor> list = new ArrayList<Adaptor> ();
		list.addAll(adaptors.values());
		return list;
	}
}
