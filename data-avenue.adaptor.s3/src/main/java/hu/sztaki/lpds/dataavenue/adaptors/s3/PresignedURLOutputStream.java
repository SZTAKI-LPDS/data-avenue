package hu.sztaki.lpds.dataavenue.adaptors.s3;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;

// Use up to 5 GB in size (if size is unknown, it will buffer in memory)
public class PresignedURLOutputStream extends OutputStream {
	private static final Logger log = LoggerFactory.getLogger(PresignedURLOutputStream.class);

	private final OutputStream outputStream;
	private final HttpURLConnection httpUrlConnection;
	
	@Override public void write(int b) throws IOException {
		outputStream.write(b);
	}
	@Override public void close() throws IOException {
		outputStream.close();
		int responseCode = httpUrlConnection.getResponseCode();
		if (responseCode != 200) throw new IOException("Unauthorized (response code: " + responseCode + ")"); 
	}
	
	public PresignedURLOutputStream(final AmazonS3Client s3Client, final String bucketName, final String keyName, final long size) throws OperationException {
		log.trace("New PresignedURLOutputStream (content length: " + size + ")");
		long expiration = System.currentTimeMillis() + 1000 * 60; // 1 minute
		GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, keyName);
		generatePresignedUrlRequest.setMethod(HttpMethod.PUT); 
		generatePresignedUrlRequest.setExpiration(new Date(expiration));
		URL url = s3Client.generatePresignedUrl(generatePresignedUrlRequest);
		HttpURLConnection connection;
		try { connection = (HttpURLConnection) url.openConnection(); } 
		catch (IOException e) { throw new OperationException("Cannot open resource output stream (cannot open connection)!", e); }
		connection.setDoOutput(true);
		try { connection.setRequestMethod("PUT"); } 
		catch (ProtocolException e) { throw new OperationException("Cannot open resource output stream (PUT not supported)!", e); }
		// if size is known, set it, otherwise data will be buffered in memory (causing out of memory exception, if large)
		if (size != 0 && size <= Integer.MAX_VALUE) connection.setFixedLengthStreamingMode((int)size); // http://stackoverflow.com/questions/2082057/java-outputstream-java-lang-outofmemoryerror-java-heap-space
		OutputStream out = null;
		try { out = connection.getOutputStream(); }
		catch (IOException e) {	throw new OperationException("Cannot open resource output stream (IOException)!", e); }
		this.outputStream = out;
		this.httpUrlConnection = connection;
	}
}
