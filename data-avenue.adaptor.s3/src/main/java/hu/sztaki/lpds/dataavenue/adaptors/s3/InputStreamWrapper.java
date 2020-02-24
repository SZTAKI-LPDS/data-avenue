package hu.sztaki.lpds.dataavenue.adaptors.s3;

import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;

import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.services.s3.AmazonS3Client;

/*
 * This class wraps S3 object input stream such that close shuts down client unless session is provided
 */
public class InputStreamWrapper extends InputStream {
	InputStream is;
	DataAvenueSession session;
	AmazonS3Client client;
	InputStreamWrapper(InputStream is, DataAvenueSession session, AmazonS3Client client) { this.is = is; this.session = session; this.client = client;}

	@Override public int read() throws IOException { return is.read(); }
	@Override public int read(byte b[]) throws IOException { return is.read(b); }
	@Override public int read(byte b[], int off, int len) throws IOException { return is.read(b, off, len); }
	@Override public void close() throws IOException {
		is.close();
		if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} 
	}
}
