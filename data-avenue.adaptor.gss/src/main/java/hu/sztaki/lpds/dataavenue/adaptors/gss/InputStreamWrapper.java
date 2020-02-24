package hu.sztaki.lpds.dataavenue.adaptors.gss;

import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;

import java.io.IOException;
import java.io.InputStream;

public class InputStreamWrapper extends InputStream {
	InputStream is;
	DataAvenueSession session;
	GSSClient client;
	InputStreamWrapper(InputStream is, DataAvenueSession session, GSSClient client) { this.is = is; this.session = session; this.client = client;}

	@Override public int read() throws IOException { return is.read(); }
	@Override public int read(byte b[]) throws IOException { return is.read(b); }
	@Override public int read(byte b[], int off, int len) throws IOException { return is.read(b, off, len); }
	@Override public void close() throws IOException {
		is.close();
		if (session == null && client != null) try { client.shutdown(); } catch (Exception x) {} 
	}
}
