package hu.sztaki.lpds.dataavenue.core.servlets;

import hu.sztaki.lpds.dataavenue.core.AdaptorRegistry;
import hu.sztaki.lpds.dataavenue.core.Configuration;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class ProtocolDetails extends HttpServlet {
	public static final String PROTOCOL = "protocol"; 
	
	@SuppressWarnings("deprecation")
	protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		request.setAttribute("version", Configuration.getVersion());
		
		Adaptor adaptor;   
		try {
			String protocol = (String) request.getParameter(PROTOCOL);
			if (protocol != null) {
				adaptor = AdaptorRegistry.getAdaptorInstance(protocol);
				 
				request.setAttribute("protocol", protocol);
				request.setAttribute("adaptorname", adaptor.getName());
				request.setAttribute("adaptorversion", adaptor.getVersion());
				request.setAttribute("adaptordescription", adaptor.getDescription());
				request.setAttribute("adaptoroperations", adaptor.getSupportedOperationTypes(protocol));
				request.setAttribute("adaptorclass", adaptor.getClass().getName());
				
				Map <String, String> authenticationTypeAndUsage = new TreeMap<String,String>();
				for (String authType : adaptor.getAuthenticationTypes(protocol)) 
					authenticationTypeAndUsage.put(authType, adaptor.getAuthenticationTypeUsage(protocol, authType));
				
				response.setContentType("text/html");
				request.setAttribute("authtypesandusage", authenticationTypeAndUsage);
			}
		} catch (NotSupportedProtocolException e) {} 
		
    	getServletContext().getRequestDispatcher("/protocoldetails.jsp").include(request, response);
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        processRequest(request, response);
    } 

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        processRequest(request, response);
    }
}