package hu.sztaki.lpds.dataavenue.adaptors.s3;

import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import java.io.IOException;
import java.io.OutputStream;
import com.amazonaws.services.s3.AmazonS3Client;

class OutputStreamWrapper extends OutputStream {
	OutputStream os;
	DataAvenueSession session;
	AmazonS3Client client;
	OutputStreamWrapper(OutputStream os,  DataAvenueSession session, AmazonS3Client client) { this.os = os; this.session = session; this.client = client; }
	@Override public void write(int b) throws IOException {	os.write(b); }
	@Override  public void write(byte b[]) throws IOException { os.write(b); }
	@Override  public void write(byte b[], int off, int len) throws IOException { os.write(b, off, len); }
	@Override  public void flush() throws IOException { os.flush(); }
	@Override public void close() throws IOException {
		os.close();
		if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} 
	}
}