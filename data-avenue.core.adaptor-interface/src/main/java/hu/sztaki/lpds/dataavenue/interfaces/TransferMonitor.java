package hu.sztaki.lpds.dataavenue.interfaces;

/**
 * @author Akos Hajnal
 *
 * Monitor contains details about running (created) tasks: state, file size, bytes transferred, and failure case, 
 * which data must be maintained by the adaptor implementation.
 * 
 * Note: the implementation of this class must be thread-safe (multiple processes access this object). 
 */
public interface TransferMonitor {

	// adaptor managed data
	public void setTotalDataSize(long size);
	public long getTotalDataSize();
	
	public void setBytesTransferred(long bytes);
	public void incBytesTransferred(long bytes);
	public long getBytesTransferred();

	public void setFailureCause(String cause); // set on failed/canceled/aborted
	public String getFailureCause(); 
	
	public void notifyBytesTransferredIncrement(long bytesTransferredSinceLastNotification); // adaptor should report byte tr. increments

	// adaptor managed status 
	public void scheduled();
	public void transferring();
	public void done();
	public void failed(String failureCause);
	public void retry();
}