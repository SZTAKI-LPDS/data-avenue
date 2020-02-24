package hu.sztaki.lpds.dataavenue.adaptors.jsaga;

import java.io.IOException;
import java.io.InputStream;

import org.ogf.saga.error.NotImplementedException;
import org.ogf.saga.file.FileInputStream;
import org.ogf.saga.task.TaskMode;

public class CloseableJSagaInputStream extends InputStream{
	final FileInputStream s;
	CloseableJSagaInputStream(FileInputStream s) { this.s = s; }
	
	@Override
	public int read() throws IOException { return s.read(); }
	
    @Override
    public void close() throws IOException { 
    	try { s.close(TaskMode.SYNC); } 
    	catch (NotImplementedException e) { s.close(); } // java.io.IOException: java.io.EOFException at fr.in2p3.jsaga.impl.file.stream.FileInputStreamImpl.close(FileInputStreamImpl.java:68) ~[jsaga-engine-0.9.16-20130916.125217-22.jar:0.9.16-SNAPSHOT]
    }
}