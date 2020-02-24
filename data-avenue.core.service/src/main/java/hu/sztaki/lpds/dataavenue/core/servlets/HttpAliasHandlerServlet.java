package hu.sztaki.lpds.dataavenue.core.servlets;

import hu.sztaki.lpds.dataavenue.core.AdaptorRegistry;
import hu.sztaki.lpds.dataavenue.core.HttpAlias;
import hu.sztaki.lpds.dataavenue.core.HttpAliasRegistry;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.core.interfaces.impl.URIFactory;
import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase.URIType;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DataAvenueSessionImpl;
import static hu.sztaki.lpds.dataavenue.adaptors.jsaga.SecurityContextHelper.*;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hu.sztaki.lpds.dataavenue.core.Utils;

@Deprecated
@SuppressWarnings("serial")
public class HttpAliasHandlerServlet extends HttpServlet {
	
	private static final Logger log = LoggerFactory.getLogger(HttpAliasHandlerServlet.class);
	
	private static final int DEFAULT_BUFFER_SIZE = 8192;
	private static final String HTTP_HEADER_CONTENT_TYPE = "Content-Type";
	private static final String HTTP_HEADER_CONTENT_TYPE_ZIP_MIME_TYPE = "application/zip";
	public static final String HTTP_HEADER_CONTENT_TYPE_GZIP_MIME_TYPE = "application/gzip";
	
	private static final String TEST_UUID = "00000000-0000-0000-0000-000000000000";
	
	private static class BadRequestException extends Exception {
		BadRequestException(String msg) { super(msg); }
	}
	private static class AliasException extends Exception {
		AliasException(Exception e) { super(e); }
	}
	private static class AliasCredentialException extends Exception {
		AliasCredentialException(Exception e) { super(e); }
	}
	
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
    	try { 
    		doDownload(request, response); 
    	} 
    	catch(BadRequestException e) { response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage()); }
    	catch(AliasCredentialException e) { response.sendError(HttpServletResponse.SC_FORBIDDEN, e.getMessage()); }
    	catch(AliasException e) { response.sendError(HttpServletResponse.SC_NOT_FOUND, e.getMessage()); }
        catch(IOException e) { response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage()); }
    }
	
    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	try { 
    		doUploadPut(request, response);
    	} 
    	catch(BadRequestException e) { response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage()); }
    	catch(AliasCredentialException e) { response.sendError(HttpServletResponse.SC_FORBIDDEN, e.getMessage()); }
    	catch(AliasException e) { response.sendError(HttpServletResponse.SC_NOT_FOUND, e.getMessage()); }
        catch(IOException e) { response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage()); }
    }
    
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	ServletOutputStream servletOut = response.getOutputStream();
    	response.setContentType("text/html; charset=UTF-8");
    	response.setCharacterEncoding("UTF-8");

    	String htmlResponse;
    	try { htmlResponse = doUploadPost(request, response); } 
    	catch (IOException e) {	
    		log.debug("POST failed" ,e); 
    		htmlResponse = errorHtml(e);
    	}
    	
    	if (servletOut != null) {
	        servletOut.write(htmlResponse.getBytes());
	        servletOut.flush(); 
	        log.debug("Closing servlet output stream"); try {  if (servletOut != null) servletOut.close(); } catch(IOException e) { log.error("IOException at closing serlet output stream: " + e.getMessage(), e); }
    	}
    }
    
    @SuppressWarnings("unused")
	private void doTestDownload(HttpServletRequest request, HttpServletResponse response) throws IOException {
    	final long bytesToWrite = 5 * 1024 * 1024 * 1024l; // 5GB
    	System.out.println("Test download: " + bytesToWrite + " (" + Long.toString(bytesToWrite) + ") bytes");
       	response.setContentType("application/octet-stream"); 
    	response.setHeader("Content-Disposition", "attachment; filename=\"5GB.dat\"");
    	response.addHeader("Content-Length", Long.toString(bytesToWrite));
    	OutputStream out = null;
    	try {
	    	out = response.getOutputStream();
			response.setStatus(HttpServletResponse.SC_OK);
			byte [] buffer = new byte[DEFAULT_BUFFER_SIZE];
			new java.util.Random().nextBytes(buffer); // randomize
		    for (int i = 0; i < bytesToWrite / DEFAULT_BUFFER_SIZE; i++) out.write(buffer, 0, DEFAULT_BUFFER_SIZE);
		    if ((int)(bytesToWrite % DEFAULT_BUFFER_SIZE) > 0) out.write(buffer, 0, (int)(bytesToWrite % DEFAULT_BUFFER_SIZE));
    	} 
		finally {
		       log.trace("### Closing output stream"); try { if (out != null) out.close(); } catch(IOException e) { log.error("IOException at closing servlet output stream: " + e.getMessage(), e); }
		}
    }
    
    private void doDownload(HttpServletRequest request, HttpServletResponse response) throws BadRequestException, AliasException, AliasCredentialException, IOException {
    	
    	log.trace("==========================================================");
    	log.info("> doDownload (GET): " + request.getParameter("id"));
    	
    	long connectionStart = System.currentTimeMillis();
    	
    	String aliasId = request.getParameter("id"); 
    	if (aliasId == null) throw new BadRequestException("Missing request parameter: id"); 
    	
    	if (TEST_UUID.equals(aliasId)) { doTestDownload(request, response); return; }
    	
    	HttpAlias alias = null;
		try { alias = HttpAliasRegistry.getInstance().getHttpAlias(aliasId); } 
		catch (Exception e) { throw new AliasCredentialException(e); } // cannot decode encrypted credential data
		 
    	if (alias == null) throw new BadRequestException("Invalid alias: " + aliasId);
    	
		URIBase uri;
		try { uri =  URIFactory.createURI(alias.getSource());	} 
		catch (URIException e) {
			alias.failed("Invalid URI", 0l, 0l);
			throw new AliasException(e);
		}
		
		Adaptor adaptor;
		try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); } 
		catch (NotSupportedProtocolException e) { 
			alias.failed("Not supported protocol", 0l, 0l);
			throw new AliasException(e);
		}

		DataAvenueSessionImpl sessionContainer = new DataAvenueSessionImpl();
		ReadOnceCreds creds = new ReadOnceCreds(alias.getCredentials()); // speed improvement by re-using session

		if (alias.getDirectURL() != null) {
			log.debug("Redirecting to URL: " + alias.getDirectURL());
			response.sendRedirect(alias.getDirectURL());
			return;
		}
		
    	String fileName;
    	try { fileName = alias.getUri().getEntryName(); } 
    	catch (URIException e) { throw new AliasException(e); }
    	log.debug("File name: " + fileName);
    	response.setContentType("application/octet-stream"); 
    	response.setHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");

    	// inform downloader about file size (if available)
    	long fileSize = 0l;
    	try {
    		fileSize = adaptor.getFileSize(uri, creds.get(), sessionContainer); // detemine content length in advance if possible
    		if (fileSize > 0l) {
    			if (fileSize <= Integer.MAX_VALUE) response.setContentLength((int)fileSize);
    			else response.addHeader("Content-Length", Long.toString(fileSize));
    		}
    		log.debug("File size: {}", fileSize);
    	} catch (Exception e) { 
    		log.warn("No file size information about resource: " + alias.getSource() + " (" + e.getMessage() + ")");
    	}
		
		InputStream in = null;
        ServletOutputStream out = null;
        long bytesTransferred = 0l;
        try {
    		try { in = adaptor.getInputStream(uri, creds.get(), sessionContainer); } 
    		catch (URIException e) { throw new AliasException(e); }
    		catch (CredentialException e) {throw new AliasCredentialException(e);	}
    		catch (OperationException e) { throw new AliasException(e); }

        	out = response.getOutputStream();
			response.setStatus(HttpServletResponse.SC_OK);
        
        	long connectionTime = System.currentTimeMillis() - connectionStart;
        	log.trace("### Download: " + uri.getURI());
			log.trace("### Connection time: " + ((float)connectionTime/1000) + " s (" + connectionTime + "ms)");
	        
        	alias.transferring(bytesTransferred, fileSize);

        	long transferStart = System.currentTimeMillis();
        	
			boolean downLinkSpeedTest = false;
			
			if (!downLinkSpeedTest) {
	            int readBytes;
	        	byte [] buffer = new byte[DEFAULT_BUFFER_SIZE];
		        while ((readBytes = in.read(buffer)) > 0) { // 26.39s
		        	out.write(buffer, 0, readBytes);
		        	bytesTransferred += readBytes;
		        }
			} else {
				
				log.warn("### TEST MODE!!!");
					
				log.debug("### Measuring input stream read speed...");
				long testStartTime = System.currentTimeMillis();
		        int readBytes;
		        byte [] buffer = new byte[DEFAULT_BUFFER_SIZE];
			    while ((readBytes = in.read(buffer)) > 0) { // 12.95s 
			       	bytesTransferred += readBytes; // count and drop
			    }
			    long splitTime = System.currentTimeMillis() - testStartTime;
		        log.debug("### Bytes read: " + bytesTransferred);
			    log.debug("### Input stream read time: " + ((float)splitTime/1000) + " s (" + splitTime + "ms)");
		        log.debug("### Input stream read speed: " + ((float)bytesTransferred/(1024*1024))/((float)splitTime/1000) + " MB/s");
					
				log.debug("### Measuring output stream write speed...");
				new java.util.Random().nextBytes(buffer); // randomize
				testStartTime = System.currentTimeMillis();
				bytesTransferred = 0l;
				long bytesToWrite = fileSize;
			    for (int i = 0; i < bytesToWrite / DEFAULT_BUFFER_SIZE; i++) { // 9.27s
			       	out.write(buffer, 0, DEFAULT_BUFFER_SIZE);
			       	bytesTransferred += DEFAULT_BUFFER_SIZE;
			    }
			    if ((int)(bytesToWrite % DEFAULT_BUFFER_SIZE) > 0) {
			       	out.write(buffer, 0, (int)(bytesToWrite % DEFAULT_BUFFER_SIZE));
			       	bytesTransferred += (int)(bytesToWrite % DEFAULT_BUFFER_SIZE);
			    }
			    splitTime = System.currentTimeMillis() - testStartTime;
		        log.debug("### Bytes written: " + bytesTransferred);
			    log.debug("### Output stream write time: " + ((float)splitTime/1000) + " s (" + splitTime + "ms)");
		        log.debug("### Output stream write speed: " + ((float)bytesTransferred/(1024*1024))/((float)splitTime/1000) + " MB/s");
			}
	        long transferTime = System.currentTimeMillis() - transferStart;

	        log.trace("### Transfer time: " + ((float)transferTime/1000) + " s (" + transferTime + "ms)");
	        log.trace("### Bytes: " + bytesTransferred);
	        log.trace("### Speed: " + ((float)bytesTransferred/(1024*1024))/((float)transferTime/1000) + " MB/s");
        } 
        catch (IOException e) {
        	log.error("IOException during download (" + uri + " " + aliasId + "): " + e.getMessage() , e);
            alias.failed("IOException during download: " + e.getMessage(), bytesTransferred, fileSize);
        	throw e;
        }
        finally {
            log.trace("### Closing output stream"); try { if (out != null) out.close(); } catch(IOException e) { log.error("IOException at closing servlet output stream: " + e.getMessage(), e); }
            log.debug("Closing input stream"); try { if (in != null) in.close(); } catch(IOException e) { log.error("IOException at closing input stream of " + uri + " (" + aliasId + "): " + e.getMessage(),  e); }
            log.debug("Closing session"); if (sessionContainer != null) sessionContainer.discard(); 
        }

        log.info("< GET download completed: " + alias.getSource() +  " " + bytesTransferred + " bytes transferred");
        
        log.trace("### servlet done");
        alias.done(bytesTransferred, fileSize);
    }
    
    private void doUploadPut(HttpServletRequest request, HttpServletResponse response) throws BadRequestException, AliasException, AliasCredentialException, IOException {
    	
    	log.trace("==========================================================");
    	log.info("> doUpload (PUT): " + request.getParameter("id"));
    	
    	long connectionStart = System.currentTimeMillis();

       	String aliasId = request.getParameter("id");
    	if (aliasId == null) {
    		throw new BadRequestException("Missing request parameter: id"); 
    	}
    	
    	HttpAlias alias = null;
		try { alias = HttpAliasRegistry.getInstance().getHttpAlias(aliasId); } 
		catch (Exception e) { 
			throw new AliasCredentialException(e); // cannot decode encrypted credential data
		}
		
    	if (alias == null) {
    		throw new BadRequestException("Invalid alias: " + aliasId);
    	}
		
    	if (alias.isForReading()) {
			throw new BadRequestException("Trying to write a resource created for reading");
    	}
		
		URIBase uri;
		try { uri =  URIFactory.createURI(alias.getSource());	} 
		catch (URIException e) { 
			alias.failed("Invalid alias source URI!", 0l, 0l);
			throw new AliasException(e);
		}
		
		Adaptor adaptor;
		try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); } 
		catch (NotSupportedProtocolException e) { 
			alias.failed("Not supported protocol!", 0l, 0l);
			throw new AliasException(e);
		}

		DataAvenueSessionImpl sessionContainer = new DataAvenueSessionImpl(); 

		if (alias.getDirectURL() != null) {
			log.debug("Redirecting to URL: " + alias.getDirectURL());
			response.sendRedirect(alias.getDirectURL());
			return;
		}
		
		// gsiftp protocol allows write-once files. to allow multiple uploads, try to delete the file first
		if (GSIFTP_PROTOCOL.equals(uri.getProtocol()) || SRM_PROTOCOL.equals(uri.getProtocol())) {
			log.debug("Trying to delete file {} (to allow multiple gsiftp/srm uploads...", uri);
			try { adaptor.delete(uri, alias.getCredentials(), sessionContainer); } 
			catch (Exception e) { log.debug("Cannot delete file ({})", e.getMessage());	}
		}
		
		long bytesTransferred = 0l;

		if (!alias.extractArchive()) { // plain upload

	    	InputStream in = null;
			OutputStream out = null;
	        try {
	    		try { out = adaptor.getOutputStream(uri, alias.getCredentials(), sessionContainer, -1);	}
	    		catch (URIException e) { throw new AliasException(e); }
	    		catch (CredentialException e) {throw new AliasCredentialException(e);	}
	    		catch (OperationException e) { throw new AliasException(e); }
	
				try { in = request.getInputStream(); } 
				catch (IOException e) { throw new IOException("Cannot open request input stream! (" + aliasId + ")", e); }	
	        	
	        	alias.transferring(bytesTransferred, 0l);

	        	long connectionTime = System.currentTimeMillis() - connectionStart;
	        	log.trace("### Upload: " + uri.getURI()); 
				log.trace("### Connection time: " + ((float)connectionTime/1000) + " s (" + connectionTime + "ms)");
		        
				long transferStart = System.currentTimeMillis();
	        	
				boolean upLinkSpeedTest = false; 

				if (!upLinkSpeedTest) {
		            int readBytes;
		        	byte [] buffer = new byte[DEFAULT_BUFFER_SIZE];
			        while ((readBytes = in.read(buffer)) > 0) { 
			        	out.write(buffer, 0, readBytes); 
			        	out.flush();
			        	bytesTransferred += readBytes;
			        }
				} else {
					log.error("TEST MODE!!!");
					int readBytes;
					byte [] buffer = new byte[DEFAULT_BUFFER_SIZE];
			        while ((readBytes = in.read(buffer)) > 0) { 
			        	bytesTransferred += readBytes;
			        }
				}
		        
		        long transferTime = System.currentTimeMillis() - transferStart;
		        log.trace("### Transfer time: " + ((float)transferTime/1000) + " s (" + transferTime + "ms)");
		        log.trace("### Bytes: " + bytesTransferred);
		        log.trace("### Speed: " + ((float)bytesTransferred/(1024*1024))/((float)transferTime/1000) + " MB/s");

	        } 
	        catch (IOException e) {
	        	log.error("IOException during upload (" + uri + " " + aliasId + "): " + e.getMessage() , e);
	            alias.failed("IOException during upload: " + e.getMessage(), bytesTransferred, 0l);
	        	throw e;
	        }
	        finally {
	            log.debug("Closing input stream"); try { if (in != null) in.close(); } catch (IOException e) { log.error("IOException at closing servlet input stream (" + aliasId + "): " + e.getMessage(), e); }
	            log.debug("Closing output stream"); try {  if (out != null) out.close(); } catch (IOException e) { log.error("IOException at closing output stream of " + uri + " (" + aliasId + "): " + e.getMessage(), e); }
	            log.debug("Closing session"); if (sessionContainer != null) sessionContainer.discard(); 
	        }
		} else { // tar.gz/zip upload
			if (uri.getType() != URIType.DIRECTORY) throw new IOException("Extract archive option requires directory target!"); // assert 
			
			InputStream in = null;
			try { in = request.getInputStream(); } 
			catch (IOException e) { throw new IOException("Cannot open request input stream! (" + aliasId + ")", e); }	
			
			boolean zip = HTTP_HEADER_CONTENT_TYPE_ZIP_MIME_TYPE.equals(request.getHeader(HTTP_HEADER_CONTENT_TYPE));
			
			@SuppressWarnings("unchecked")
			Enumeration<String> headerNames = request.getHeaderNames();
			if (headerNames != null) while (headerNames.hasMoreElements()) {
				StringBuilder sb = new StringBuilder();
				String headerName = headerNames.nextElement();
				sb.append("HEADER " + headerName + ":");
				@SuppressWarnings("unchecked")
				Enumeration<String> headers = request.getHeaders(headerName);
				if (headers != null) while (headers.hasMoreElements()) {
					sb.append(" " + headers.nextElement());
				}
				log.trace(sb.toString());
			}
			
			try { bytesTransferred = tarGzZipReader(in, adaptor, alias, zip); } 
			catch (IOException e) {
				alias.failed(e.getMessage(), bytesTransferred, 0l);
				throw e; 
			}
			finally {
			       log.debug("Closing input stream"); try { if (in != null) in.close(); } catch(IOException e) { log.error("IOException at closing input stream: " + e.getMessage(), e); }
			}
		}
		
        log.info("< PUT upload completed: " + alias.getSource() +  " " + bytesTransferred + " bytes transferred");
        alias.done(bytesTransferred, bytesTransferred); // if completed, size is bytesTransferred
    }
    
    private String doUploadPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

    	log.trace("==========================================================");
    	log.info("> doUpload (POST): " + request.getParameter("id"));
    	
    	String aliasId = request.getParameter("id");
    	if (aliasId == null) throw new IOException("Missing request parameter: id!");
    	
    	HttpAlias alias = null;
		try { alias = HttpAliasRegistry.getInstance().getHttpAlias(aliasId); } 
		catch (Exception e) { throw new IOException("Cannot retrieve alias data! (" + aliasId + ")", e); } // e.g., cannot decode encrypted credential data
		
    	if (alias == null) throw new IOException("Invalid alias id! (" + aliasId + ")");
		if (alias.isForReading()) throw new IOException("Trying to write a resource that has been created for reading! (" + aliasId + ")");
		
		URIBase uri;
		try { uri =  URIFactory.createURI(alias.getSource());	} 
		catch (URIException e) { 
			alias.failed("Invalid alias source URI!", 0l, 0l);
			throw new IOException("Invalid alias source URI! (" + alias.getSource() + ")", e);
		}
		
		Adaptor adaptor;
		try { adaptor = AdaptorRegistry.getAdaptorInstance(uri.getProtocol()); } 
		catch (NotSupportedProtocolException e) { 
			alias.failed("Not supported protocol!", 0l, 0l);
			throw new IOException("Not supported protocol! (" + uri.getProtocol() + ")", e);
		}
    	
		boolean isMultipartContent = ServletFileUpload.isMultipartContent(request);
		if (!isMultipartContent) throw new IOException("Multipart content expected! (" + aliasId + ")");

		ServletFileUpload uploadStream = new ServletFileUpload(); // uses streaming 
		
		FileItemIterator iter = null;
		try { iter = uploadStream.getItemIterator(request); } 
		catch (IOException e) {	throw new IOException("IOException on getting multipart content item iterator! (" + aliasId + ")", e);	}
		catch (FileUploadException e) {	throw new IOException("FileUploadException on getting multipart content item iterator! (" + aliasId + ")", e);	}
		
		FileItemStream item = null;
		try {
			while (iter.hasNext()) {
			    item = iter.next();
			    if (item.isFormField() == false) { // uploaded file
			    	log.debug("Field name: '" + item.getFieldName() + "', file name: '" + item.getName() +"'");
			    	break;
			    }
			}
		} 
		catch (IOException e) { throw new IOException("IOException on iterating over multipart content! (" + aliasId + ")", e); }
		catch (FileUploadException e) { throw new IOException("FileUploadException on iterating over multipart content! (" + aliasId + ")", e); }
		
		if (item == null) throw new IOException("Missing file form field! (" + aliasId + ")");
		
		DataAvenueSessionImpl sessionContainer = new DataAvenueSessionImpl();; 
		
		// gsiftp protocol allows write-once files. to allow multiple uploads, try to delete the file first
		if (GSIFTP_PROTOCOL.equals(uri.getProtocol()) || SRM_PROTOCOL.equals(uri.getProtocol())) {
			log.debug("Trying to delete file {} (to allow multiple gsiftp/srm uploads...", uri);
			try { adaptor.delete(uri, alias.getCredentials(), sessionContainer); } 
			catch (Exception e) { log.debug("Cannot delete file ({})", e.getMessage());	}
		}

        long bytesTransferred = 0l;

//         POST does not accept extractArchive aliases
//        if (item.getName().toLowerCase().endsWith(".zip") || item.getName().toLowerCase().endsWith("tar.gz")) alias.setExtractArchive(true); 
        
		if (!alias.extractArchive()) { // plain upload

			InputStream in = null;
			OutputStream out = null;
			try { // now we open streams, close in finally
				
				try { in = item.openStream(); } 
				catch (IOException e) { throw new IOException("Cannot open input stream of file form field! (" + aliasId + ")", e); }	
	
	    		try { out = adaptor.getOutputStream(uri, alias.getCredentials(), sessionContainer, -1); } 
	    		catch (Exception e) { throw new IOException("Cannot open output stream for resource!", e); } 
				
	        	alias.transferring(bytesTransferred, 0l);
	        	
	            int readBytes;
	        	byte [] buffer = new byte[DEFAULT_BUFFER_SIZE];
		        try {
			        while ((readBytes = in.read(buffer)) > 0) { 
			        	out.write(buffer, 0, readBytes);
			            out.flush();
			        	bytesTransferred += readBytes;
			        }
		        } catch (IOException e) { throw new IOException("Cannot read input stream or write output stream! (" + aliasId + ")", e); }
			}
			catch (IOException e) {
				alias.failed(e.getMessage(), bytesTransferred, 0l);
				throw e;
			}
			finally {
		        log.debug("Closing input stream"); try { if (in != null) in.close(); } catch(IOException e) { log.error("IOException at closing input stream: " + e.getMessage(), e); }
		        log.debug("Closing output stream"); try {  if (out != null) out.close(); } catch (IOException e) { log.error("IOException at closing output stream: " + e.getMessage(), e); }
		        log.debug("Closing session"); if (sessionContainer != null) sessionContainer.discard();
			}
			
		} else { // tar.gz/zip upload

			// workaround: replace alias file name with alias path
			log.debug("Original alias source: " + alias.getSource());
			if (!alias.getSource().endsWith("/")) alias.setSource(alias.getSource().substring(0, alias.getSource().lastIndexOf("/") + 1));
			log.debug("Alias directory source: " + alias.getSource());
			try { uri = URIFactory.createURI(alias.getSource()); } catch (URIException e) {}

			if (uri.getType() != URIType.DIRECTORY) throw new IOException("Extract archive option requires directory target!"); // assert 
				
			InputStream in = null;
			try { in = item.openStream(); } 
			catch (IOException e) { throw new IOException("Cannot open input stream of file form field! (" + aliasId + ")", e); }	
				
			try { bytesTransferred = tarGzZipReader(in, adaptor, alias, item.getName().toLowerCase().endsWith(".zip")); } 
			catch (IOException e) {
				alias.failed(e.getMessage(), bytesTransferred, 0l);
				throw e; 
			}
			finally {
			       log.debug("Closing input stream"); try { if (in != null) in.close(); } catch(IOException e) { log.error("IOException at closing input stream: " + e.getMessage(), e); }
			}
		} 
		
        alias.done(bytesTransferred, bytesTransferred); // size is bytesTransferred on completion
        log.debug("< POST upload completed: " + alias.getSource() +  " (" + bytesTransferred + " bytes transferred)");
        return okHtml(uri, bytesTransferred);
    }
    
    /**
     * Returns a short description of the servlet.
     * 
     * @return a String containing servlet description
     */
    @Override public String getServletInfo() { return "Resource download/upload servlet";  }
    
    private String okHtml(URIBase uri, long bytes) {
    	return "<!DOCTYPE html><html><head><title>Upload Completed</title></head><body>" +
    			"<font face=\"verdana\" size=\"3\" color=\"green\"><b>Upload completed!</b></font><br/><br/>" +
    			"<font face=\"verdana\" size=\"2\">" +
    			"URI: <b>" + uri.getURI() + "</b><br/>" +
    			"Size: " + bytes + " bytes<br/>" +
    			"Date: " + Utils.dateString(System.currentTimeMillis()) + "" +
    			"</font></body></html>";
    }

    private String errorHtml(Exception e) {
    	return "<!DOCTYPE html><html><head><title>Upload Failed</title></head><body>" +
    			"<font face=\"verdana\" size=\"3\" color=\"red\"><b>Upload failed!</b></font><br/><br/>" +
    			"<font face=\"verdana\" size=\"2\">" +
    			"Failure: <b>" + Utils.getExceptionStringWithOneCause(e) + "</b><br/>" +
    			"Date: " + Utils.dateString(System.currentTimeMillis()) + "" +
    			"</font></body></html>";
    }
    
	private class ReadOnceCreds {
		Credentials creds;
		ReadOnceCreds(Credentials creds) {
			this.creds = creds;
		}
		Credentials get() {
			if (creds != null) {
				Credentials tmp = this.creds;
				this.creds = null;
				return tmp;
			} else return null;
		}
	}
    
	private long tarGzZipReader(InputStream in, Adaptor adaptor, HttpAlias alias, boolean zip) throws IOException {
		log.trace("Extracting archive...");
		
		if (zip) log.trace("ZIP"); else log.trace("TAR.GZ");

		ArchiveInputStream archStream = null;
		try { archStream = new ArchiveStreamFactory().createArchiveInputStream(zip ? new BufferedInputStream(in) : new BufferedInputStream(new GzipCompressorInputStream(in))); } 
		catch (ArchiveException e) { throw new IOException("Archiver name is unknown!", e); }
		catch (IllegalArgumentException e) { throw new IOException("Archive input stream does not support marks!", e); }

    	byte [] buffer = new byte[DEFAULT_BUFFER_SIZE];
    	long totalBytesTransferred = 0;
    	DataAvenueSessionImpl sessionContainer = new DataAvenueSessionImpl();
    	ReadOnceCreds creds = new ReadOnceCreds(alias.getCredentials()); // speed improvement by re-using session

    	try {
    		ArchiveEntry entry = null;
			try { entry = archStream.getNextEntry();	} 
			catch (IOException e) { throw new IOException("IOException at reading archive"); }
			
			if (entry == null) throw new IOException("No entry found in archive!");
			
			String entryURIString;
			URIBase entryURI;
			Set<String> dirsCreated = new HashSet<String>();
			
			while (entry != null) {
				if (entry.isDirectory()) { // if dir

		        	// create directory
		    		entryURIString = alias.getSource() + entry.getName() + (entry.getName().endsWith("/") ? "" : "/");
		    		log.debug("> Creating archive directory: " + entryURIString);

		    		if (!dirsCreated.contains(entryURIString)) {
		    			
		    			try { entryURI =  URIFactory.createURI(entryURIString); } 
		    			catch (URIException e) { throw new IOException("Invalid entry URI: " + entryURIString, e); }
			    		
		    			try { adaptor.mkdir(entryURI, creds.get(), sessionContainer); } 
			    		catch (Exception e) { // silently ignore "dir already created" exceptions 
			    			log.debug("Exception at creating directory: " + e.getMessage()); 
			    			// throw new IOException("Exception at creating directory: " + entryURIString, e); 
			    		} 
		    			
		    			dirsCreated.add(entryURIString);
    				}
					
				} else { // if file
					
	        		// exctract file
	    			entryURIString = alias.getSource() + entry.getName();
	    			log.debug("> Extracting archive file entry: " + entryURIString);
	    			
	    			try { entryURI =  URIFactory.createURI(entryURIString); } 
	    			catch (URIException e) { throw new IOException("Invalid entry URI: " + entryURIString); }
	    			
	    			// create entry's container directory if needed
	    			if (entry.getName().contains("/")) { // entry within a subdirectory
	    				String entryDirectory = entryURIString.substring(0, entryURIString.lastIndexOf("/") + 1); // full URI without file name
	    				
	    				if (!dirsCreated.contains(entryDirectory)) {
	    					log.debug("Creating entry's container subdirectory: " + entryDirectory);
	    					// create subdir
	    					URIBase entryDirectoryURI = null;
	    	    			try { entryDirectoryURI = URIFactory.createURI(entryDirectory); } 
	    	    			catch (URIException e) { throw new IOException("Invalid entry URI: " + entryURIString, e); }
	    					try { adaptor.mkdir(entryDirectoryURI, creds.get(), sessionContainer); }
	    		    		catch (Exception e) { // silently ignore "dir already created" exceptions 
	    		    			log.debug("Exception at creating directory: " + e.getMessage()); 
	    		    			// throw new IOException("Exception at creating directory: " + entryURIString, e); 
	    		    		} 
	    					dirsCreated.add(entryDirectory);
	    				}
	    			}
	    			
		            OutputStream out = null;
		    		try { out = adaptor.getOutputStream(entryURI, creds.get(), sessionContainer, -1); } 
		    		catch (Exception e) { throw new IOException("Exception at getting output stream:" + entryURIString, e); }
		        	
		            try {
		                int readBytes = 0;
		                while ((readBytes = archStream.read(buffer)) > 0) {
		                	out.write(buffer, 0, readBytes);
		    	        	totalBytesTransferred += readBytes;
		                }
		            } 
		            catch (Exception e) { throw new IOException("Exception at getting output stream:" + entryURIString, e); }
		            finally {
		            	if (out != null) try { out.close(); } catch (IOException e){ log.error("Cannot close output stream!", e); }
		            }
				}
				
				try { entry = archStream.getNextEntry();	} 
				catch (IOException e) { throw new IOException("IOException at reading archive"); }
			}
    	}
		finally { // just to make sure zip stream is closed
			if (archStream != null) try { archStream.close(); } catch (IOException e){ log.error("Cannot close input stream!", e); }
			if (sessionContainer != null) sessionContainer.discard();
		}
    	
    	return totalBytesTransferred;
	}
}