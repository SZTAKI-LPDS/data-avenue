package hu.sztaki.lpds.dataavenue.core.filters;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.tuckey.web.filters.urlrewrite.UrlRewriteFilter;
import org.tuckey.web.filters.urlrewrite.UrlRewriteWrappedResponse;
import org.tuckey.web.filters.urlrewrite.UrlRewriter;

// NOT USED!
@Deprecated
public class UUIDUrlRewriteFilter extends UrlRewriteFilter {
	private static final String EXCLUDE_PATH = "rest";
	
	// custom filter that omits filtering paths starting with EXCLUDE_PATH
	@Override public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException {
		final HttpServletRequest hsRequest = (HttpServletRequest) request;
        final HttpServletResponse hsResponse = (HttpServletResponse) response;
        final String uri = hsRequest.getRequestURI();
         if (uri != null && uri.startsWith(EXCLUDE_PATH)) {
        	 UrlRewriter urlRewriter = getUrlRewriter(request, response, chain);
        	 UrlRewriteWrappedResponse urlRewriteWrappedResponse = new UrlRewriteWrappedResponse(hsResponse, hsRequest, urlRewriter);
        	 chain.doFilter(hsRequest, urlRewriteWrappedResponse); // continue with the chain
         } else {
        	 super.doFilter(request, response, chain); // do filtering
         }
    }

}
