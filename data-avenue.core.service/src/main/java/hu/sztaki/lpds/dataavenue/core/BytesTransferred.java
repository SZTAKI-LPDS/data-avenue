package hu.sztaki.lpds.dataavenue.core;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity 
public class BytesTransferred {
	public static final int ROW_ID = 0;
	
	@Id
	private int id = ROW_ID; // this table has exactly 1 row with id = 0
	public int getId() { return id; }
	public void setId(int id) { this.id = id; }
	
	long bytesTransferred;
	public long getBytesTransferred() { return bytesTransferred; }
	public void setBytesTransferred(long bytesTransferred) { this.bytesTransferred = bytesTransferred; }

	long since = System.currentTimeMillis();
	public long getSince() { return since; }
	public void setSince(long since) { this.since = since; }
}