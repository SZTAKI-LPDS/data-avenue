package hu.sztaki.lpds.dataavenue.core.servlets;

import hu.sztaki.lpds.dataavenue.core.TicketManager;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.UnexpectedException;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class KeyList extends HttpServlet {
	private static final Logger log = LoggerFactory.getLogger(KeyList.class);
	
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        processRequest(request, response);
    } 

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        processRequest(request, response);
    }
	
	protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try { 
			request.setAttribute("keys", TicketManager.getInstance().getAllTickets()); 
		} catch(UnexpectedException e) { log.error("Cannot query keys: " + e.getMessage()); } // no keys will be listed
		response.setContentType("text/html");
    	getServletContext().getRequestDispatcher("/WEB-INF/jsps/keys.jsp").include(request, response);
    }
}