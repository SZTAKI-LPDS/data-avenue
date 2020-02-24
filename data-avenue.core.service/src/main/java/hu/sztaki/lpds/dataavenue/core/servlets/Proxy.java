package hu.sztaki.lpds.dataavenue.core.servlets;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;
import org.ogf.saga.context.Context;
import org.ogf.saga.context.ContextFactory;
import org.ogf.saga.session.Session;
import org.ogf.saga.session.SessionFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class Proxy extends HttpServlet {
	enum CONFIG_ACTIONS {
		PROXY, 
		VOMS
	};
	
	private static final Logger log = LoggerFactory.getLogger(Proxy.class);
	
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html");
    	getServletContext().getRequestDispatcher("/WEB-INF/jsps/proxy.jsp").include(request, response);
    } 

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        processRequest(request, response);
    }
    
	protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try { if (System.getProperty("saga.factory") == null) System.setProperty("saga.factory", "fr.in2p3.jsaga.impl.SagaFactoryImpl"); } 
		catch (SecurityException x) {	log.error("Cannot set system property 'saga.factory'"); }

		String proxyPath = System.getProperty("java.io.tmpdir") + "/x509up_" + new Random().nextInt(1000000);
		//File proxy = File.createTempFile("proxy", null); // => Invalid buffer exception
		
		File usercertTempFile = null, userkeyTempFile = null; 
		try {

			String actionParam = null;
			String password = null, lifetime = null, server = null, vo = null, vomslifetime = null;
			String usercertPath = null, userkeyPath = null;

			usercertTempFile = File.createTempFile("usercert_", "pem");  
			usercertPath = usercertTempFile.getAbsolutePath();
			userkeyTempFile = File.createTempFile("userkey_", "pem");  
			userkeyPath = userkeyTempFile.getAbsolutePath();

    		// parse parameters
    		DiskFileItemFactory factory = new DiskFileItemFactory();
			factory.setSizeThreshold(10000);
			ServletFileUpload upload = new ServletFileUpload(factory);
			upload.setSizeMax(10000);
			List<FileItem> fileItems = upload.parseRequest(request);
			Iterator<FileItem> i = fileItems.iterator();
			while(i.hasNext()) {
                FileItem fi = (FileItem)i.next();
                if(fi.isFormField()) {
                	if ("action".equals(fi.getFieldName())) actionParam = fi.getString();
                	else if ("password".equals(fi.getFieldName())) password = fi.getString();
                	else if ("lifetime".equals(fi.getFieldName())) lifetime = fi.getString();
                	else if ("server".equals(fi.getFieldName())) server = fi.getString();
                	else if ("vo".equals(fi.getFieldName())) vo = fi.getString();
                	else if ("vomslifetime".equals(fi.getFieldName())) vomslifetime = fi.getString();
                	log.debug(fi.getFieldName() + ": " + ("password".equals(fi.getFieldName()) ? "***" : fi.getString()));
                } else {
                	if ("usercert".equals(fi.getFieldName())) {
                		InputStream fis = fi.getInputStream();
                		OutputStream fos = new FileOutputStream(usercertTempFile);
                		try { Streams.copy(fis, fos, true); } 
                		finally {
                			fis.close();
                			fos.close();
                		}
                	} else if ("userkey".equals(fi.getFieldName())) {
                		InputStream fis = fi.getInputStream();
                		OutputStream fos = new FileOutputStream(userkeyTempFile);
                		try { Streams.copy(fis, fos, true); } 
                		finally {
                			fis.close();
                			fos.close();
                		}
                	}
                    log.debug(fi.getFieldName() + ": " + fi.getName() + " / " + fi.getContentType() + " / " + fi.getSize());
                }
			}
    		
			setPermissions(usercertTempFile);
			setPermissions(userkeyTempFile);
			
			CONFIG_ACTIONS action = CONFIG_ACTIONS.valueOf(actionParam);
			Session session = null;
			Context ctx = null;
			switch (action) {
				case PROXY:
					session = SessionFactory.createSession(false);
					ctx = ContextFactory.createContext("Globus");
					ctx.setAttribute(Context.USERCERT, usercertPath);
					ctx.setAttribute(Context.USERKEY,  userkeyPath);
					ctx.setAttribute(Context.USERPASS, password);
					ctx.setAttribute(Context.LIFETIME, lifetime);
					ctx.setAttribute(Context.USERPROXY, proxyPath);
					
					session.addContext(ctx);
					session.close();
					break;
				case VOMS:
					session = SessionFactory.createSession(false);
					ctx = ContextFactory.createContext("VOMS");
					ctx.setAttribute(Context.USERCERT, usercertPath);
					ctx.setAttribute(Context.USERKEY, userkeyPath);
					ctx.setAttribute(Context.USERPASS, password);
					ctx.setAttribute(Context.SERVER, server); 
					ctx.setAttribute(Context.USERVO, vo); 
					ctx.setAttribute(Context.LIFETIME, vomslifetime);
					ctx.setAttribute(Context.USERPROXY, proxyPath); 
					session.addContext(ctx);
					session.close();
					break;
				default: 
					throw new IOException("Invalid action parameter: " + action);
			}

	    	response.setContentType("application/octet-stream"); 
	    	response.setHeader("Content-Disposition", "attachment; filename=\"" + "x509up" + (action == CONFIG_ACTIONS.VOMS ? ".voms" : "") + "\"");
	    	
	    	OutputStream out = null;
	    	InputStream in = null;
	    	try { 
	    		out = response.getOutputStream();
	    		in = new FileInputStream(proxyPath);
	    		int b;
		        while ((b = in.read()) > 0) out.write(b);
	    	} 
	        finally {
	            log.debug("Closing output stream"); try { if (out != null) out.close(); } catch(IOException e) { log.error(e.getMessage(), e); }
	            log.debug("Closing input stream"); try { if (in != null) in.close(); } catch(IOException e) { log.error(e.getMessage(), e); }
	        }
		}
    	catch(Exception e) {
    		log.error(e.getMessage() , e);
    		throw new IOException(e);
    	}
		finally {
			if (usercertTempFile != null) usercertTempFile.delete();
			if (userkeyTempFile != null) userkeyTempFile.delete();
			new File(proxyPath).delete();
		}
	}
	
    private void setPermissions(File temp) { // 400
    	try {
			temp.setReadable(false, false);
			temp.setWritable(false, false);
			temp.setExecutable(false, false);
			temp.setReadable(true, true);
			temp.setWritable(false, true);
			temp.setExecutable(false, false);
	    } catch (Throwable e) {	log.error("Cannot change file permissions: " + e.getMessage());  }
    }
}