package hu.sztaki.lpds.dataavenue.adaptors.jsaga;

import java.io.IOException;
import java.io.OutputStream;
import org.ogf.saga.error.NotImplementedException;
import org.ogf.saga.file.FileOutputStream;
import org.ogf.saga.task.TaskMode;

public class ClosableJSagaOutputStream extends OutputStream {
	
	final FileOutputStream s;
	ClosableJSagaOutputStream(FileOutputStream s) {	this.s = s;	}

	@Override
	public void write(int b) throws IOException { s.write(b); }
	
	@Override
    public void flush() throws IOException {
    	try { s.flush(TaskMode.SYNC); } 
    	catch (NotImplementedException e) { s.flush(); }
    }

    @Override
    public void close() throws IOException {
    	try { s.close(TaskMode.SYNC); } 
    	catch (NotImplementedException e) { s.close(); }
    }
}
