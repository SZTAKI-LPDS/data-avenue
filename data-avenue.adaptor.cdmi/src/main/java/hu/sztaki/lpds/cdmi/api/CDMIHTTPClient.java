package hu.sztaki.lpds.cdmi.api;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

@SuppressWarnings("deprecation")
public class CDMIHTTPClient {
//	fxime create client manager
//	static HttpClient client;
	static {
//		HttpParams params = new BasicHttpParams();
//	    SchemeRegistry registry = new SchemeRegistry();
//	    registry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
//	    ClientConnectionManager cm = new PoolingClientConnectionManager(registry);
//	    HttpClient client = new DefaultHttpClient(cm, params);
	    
//	    global_c is initialized once: HttpClient global_c = new HttpClient(new MultiThreadedHttpConnectionManager());
//	 	try { global_c.executeMethod(method); } catch(...) {}
//	 	finally { method.releaseConnection(); }
	}
	static HttpClient getClient() {
		return new DefaultHttpClient(); // FIXME use pool
	}
}
