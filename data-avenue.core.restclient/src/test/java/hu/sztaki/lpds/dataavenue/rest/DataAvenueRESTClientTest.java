package hu.sztaki.lpds.dataavenue.rest;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.JSONArray;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

@Deprecated
public class DataAvenueRESTClientTest {
	private DataAvenueRESTClient c; 
	private Properties prop;
	private String sftpDir, s3Dir, gridftpDir;
	private Map<String, String> sftpCreds, s3Creds, gridftpCreds;

	@Before	public void setUp() {
		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties"); // emits no exception but null returned
		if (in == null) { System.out.println("File not found: config.properties"); return; }
		else {
			prop = new Properties();  
			try { 
				prop.load(in);
				in.close();
				c = new DataAvenueRESTClient.Builder(prop.getProperty("url"), prop.getProperty("key")).withDNS(prop.getProperty("dns")).build();
				sftpDir = prop.getProperty("sftpDirURI");
				sftpCreds = new HashMap<String, String>();
				sftpCreds.put(DataAvenueRESTClient.HTTP_HEADER_USERNAME, (String) prop.get("sftpUsername"));
				sftpCreds.put(DataAvenueRESTClient.HTTP_HEADER_PASSWORD, (String) prop.get("sftpPassword"));
				s3Dir = prop.getProperty("s3DirURI");
				s3Creds = new HashMap<String, String>();
				s3Creds.put(DataAvenueRESTClient.HTTP_HEADER_USERNAME, (String) prop.get("s3Username"));
				s3Creds.put(DataAvenueRESTClient.HTTP_HEADER_PASSWORD, (String) prop.get("s3Password"));
				gridftpDir = prop.getProperty("gridftpDirURI");
				gridftpCreds = new HashMap<String, String>();
				gridftpCreds.put(DataAvenueRESTClient.HTTP_HEADER_USERNAME, (String) prop.get("griftpProxy"));
				
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Exception: " + e.getMessage()); 
			}
		}
	}  
	
	@Test
	public void meta() throws Exception {
		Assume.assumeTrue(c != null);
		JSONArray a = c.protocols();
		if (a.length() > 0)
			c.authentications(a.getString(0));
		if (a.length() > 0)
			c.operations(a.getString(0));
		c.version();
	}

	@Test
	public void listSFTP() throws Exception {
		Assume.assumeTrue(c != null && sftpDir != null);
		c.list(sftpDir, sftpCreds);
	}

	@Test
	public void listS3() throws Exception {
		Assume.assumeTrue(c != null && s3Dir != null);
		c.list(s3Dir, s3Creds);
	}

	@Test
	public void listGridFTP() throws Exception {
		Assume.assumeTrue(c != null && gridftpDir != null);
		c.list(gridftpDir, gridftpCreds);
	}

	@Test
	public void mkdirSFTP() throws Exception {
		Assume.assumeTrue(c != null && sftpDir != null);
		try {
			c.rmdir(sftpDir + "rest_test/", sftpCreds);
		} catch (Exception e) {
		}
		c.mkdir(sftpDir + "rest_test/", sftpCreds);
		c.rmdir(sftpDir + "rest_test/", sftpCreds);
	}

	@Test
	public void mkdirS3() throws Exception {
		Assume.assumeTrue(c != null && s3Dir != null);
		try {
			c.rmdir(s3Dir + "rest_test/", s3Creds);
		} catch (Exception e) {
		}
		c.mkdir(s3Dir + "rest_test/", s3Creds);
		c.rmdir(s3Dir + "rest_test/", s3Creds);
	}

	@Test
	public void mkdirGridFTP() throws Exception {
		Assume.assumeTrue(c != null && gridftpDir != null);
		try { c.rmdir(gridftpDir + "rest_test/", gridftpCreds); } catch (Exception e) {}
		c.mkdir(gridftpDir + "rest_test/", gridftpCreds);
		c.rmdir(gridftpDir + "rest_test/", gridftpCreds);
	}

	@Test
	public void attributesSFTP() throws Exception {
		Assume.assumeTrue(c != null && sftpDir != null);
		c.attributes(sftpDir, sftpCreds);
	}

	@Test
	public void attribtutesS3() throws Exception {
		Assume.assumeTrue(c != null && s3Dir != null);
		c.attributes(s3Dir, s3Creds);
	}

	@Test
	public void attributesGridFTP() throws Exception {
		Assume.assumeTrue(c != null && gridftpDir != null);
		c.attributes(gridftpDir, gridftpCreds);
	}

	@Test
	public void uploadToS3() throws Exception {
		Assume.assumeTrue(c != null && s3Dir != null);
		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties"); // emits no exception but null returned
		Assume.assumeTrue(in != null);
		try { c.rmdir(s3Dir + "rest_test/", s3Creds); } catch (Exception e) {}
		c.mkdir(s3Dir + "rest_test/", s3Creds);
		c.upload(s3Dir + "rest_test/config.properties", s3Creds, in);
		try { c.rmdir(s3Dir + "rest_test/", s3Creds); } catch (Exception e) {}
	}
	
	@Test
	public void downloadFromToS3() throws Exception {
		Assume.assumeTrue(c != null && s3Dir != null);
		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties"); // emits no exception but null returned
		Assume.assumeTrue(in != null);
		try { c.rmdir(s3Dir + "rest_test/", s3Creds); } catch (Exception e) {}
		c.mkdir(s3Dir + "rest_test/", s3Creds);
		c.upload(s3Dir + "rest_test/config.properties", s3Creds, in);
		c.download(s3Dir + "rest_test/config.properties", s3Creds, "config.properties.delete");
		try { c.rmdir(s3Dir + "rest_test/", s3Creds); } catch (Exception e) {}
	}	
}
