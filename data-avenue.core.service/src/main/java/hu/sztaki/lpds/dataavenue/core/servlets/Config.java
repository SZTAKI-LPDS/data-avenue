package hu.sztaki.lpds.dataavenue.core.servlets;

import hu.sztaki.lpds.dataavenue.core.Configuration;
import hu.sztaki.lpds.dataavenue.core.HttpAliasCredentialsEncoder;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class Config extends HttpServlet {
	enum CONFIG_ACTIONS {
		ENABLE_ACCEPT_COMMANDS, 
		DISABLE_ACCEPT_COMMANDS, 
		ENABLE_ACCEPT_COPY_COMMANDS, 
		DISABLE_ACCEPT_COPY_COMMANDS, 
		ENABLE_ACCEPT_HTTP_ALIASES,
		DISABLE_ACCEPT_HTTP_ALIASES,
		SET_HTTP_ALIAS_ENCRYPTION_KEY
	};
	
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
		String actionParam = request.getParameter("action");
		if (actionParam != null) {
			try {
				CONFIG_ACTIONS action = CONFIG_ACTIONS.valueOf(actionParam);
				switch (action) {
					case ENABLE_ACCEPT_COMMANDS:
						Configuration.setAcceptCommands(true);
						break;
					case DISABLE_ACCEPT_COMMANDS:
						Configuration.setAcceptCommands(false);
						break;
					case ENABLE_ACCEPT_COPY_COMMANDS: 
						Configuration.setAcceptCopyCommands(true);
						break;
					case DISABLE_ACCEPT_COPY_COMMANDS: 
						Configuration.setAcceptCopyCommands(false);
						break;
					case ENABLE_ACCEPT_HTTP_ALIASES:
						Configuration.setAcceptHttpAliases(true);
						break;
					case DISABLE_ACCEPT_HTTP_ALIASES:
						Configuration.setAcceptHttpAliases(false);
						break;
					case SET_HTTP_ALIAS_ENCRYPTION_KEY:
						String key = request.getParameter("key");
						if (key == null) log.error("Set http alias encryption key action without key parameter (null)!");
						else { 
							Configuration.setAliasCredentialsEncriptionKey(key);
							HttpAliasCredentialsEncoder.getInstance().initCipher(key);
							log.info("Http alisas encryption key has been changed");
						}
						break;
					default: log.error("Unprocessed action parameter: " + action);
				}
			} catch (IllegalArgumentException e) {
				log.warn("Illegal action parameter for Config servlet: " + actionParam);
			}
		}

		request.setAttribute("acceptCommands", Configuration.getAcceptCommands());
		request.setAttribute("acceptCopyCommands", Configuration.getAcceptCopyCommands());
		request.setAttribute("acceptHttpAliases", Configuration.getAcceptHttpAliases());
		request.setAttribute("key", Configuration.getAliasCredentialsEncriptionKey());
		
		response.setContentType("text/html");
    	getServletContext().getRequestDispatcher("/WEB-INF/jsps/config.jsp").include(request, response);
    }
}