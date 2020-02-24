package hu.sztaki.lpds.dataavenue.core.servlets;

import hu.sztaki.lpds.dataavenue.core.HttpAliasRegistry;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Deprecated
@SuppressWarnings("serial")
public class HttpAliasList extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        processRequest(request, response);
    } 

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        processRequest(request, response);
    }
	
	protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		request.setAttribute("aliases", HttpAliasRegistry.getInstance().getAllHttpAliases());
		response.setContentType("text/html");
    	getServletContext().getRequestDispatcher("/WEB-INF/jsps/httpaliaslist.jsp").include(request, response);
    }
}