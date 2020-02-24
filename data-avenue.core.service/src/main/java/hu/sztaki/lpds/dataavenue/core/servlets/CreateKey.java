package hu.sztaki.lpds.dataavenue.core.servlets;

import hu.sztaki.lpds.dataavenue.core.TicketManager;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class CreateKey extends HttpServlet {
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
		log.debug("action: " + request.getParameter("action"));
		try {
			if ("create".equals(request.getParameter("action"))) {
				log.debug("create action");
				TicketManager.createKey((String)request.getParameter("id"), (String)request.getParameter("type"), (String)request.getParameter("name"), (String)request.getParameter("company"), (String)request.getParameter("email"));
			}
		} catch (Exception e) {
			request.setAttribute("id", request.getParameter("id"));
			request.setAttribute("type", request.getParameter("type"));
			request.setAttribute("name", request.getParameter("name"));
			request.setAttribute("company", request.getParameter("company"));
			request.setAttribute("email", request.getParameter("email"));
			request.setAttribute("error", e.getMessage());
			log.error("Create key error: " +  e.getMessage());
		}
		response.setContentType("text/html");
    	getServletContext().getRequestDispatcher("/WEB-INF/jsps/createkey.jsp").include(request, response);
    }
}