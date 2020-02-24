package hu.sztaki.lpds.dataavenue.core;

import hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum;
import hu.sztaki.lpds.dataavenue.interfaces.TransferStateEnum;

public class TransferDetails {

	public TransferDetails() {}
	
	public TransferDetails(ExtendedTransferMonitor monitor) {
		setSource(monitor.getSource());
		setTarget(monitor.getDestination());

		setOperation(monitor.getOperation());
		setState(monitor.getState());
		setFailureCause(monitor.getFailureCause());

		setTotalDataSize(monitor.getTotalDataSize());
		setBytesTransferred(monitor.getBytesTransferred());
		setCreated(monitor.getCreated());
		setNow(System.currentTimeMillis());
		setStarted(monitor.getStarted());
		setEnded(monitor.getEnded());
	} 

	private TransferStateEnum state = TransferStateEnum.CREATED; // task status
	public TransferStateEnum getState() { return state; }
	public void setState(TransferStateEnum state) { this.state = state; }

	private OperationsEnum operation; // can be one of the adaptors supported operation types (COPY/MOVE)
	public void setOperation(OperationsEnum operation) { this.operation = operation; }
	public OperationsEnum getOperation() {	return operation; }
	
	private String source;
	public String getSource() { return source; }
	public void setSource(String source) { this.source = source; }
	
	private String target;
	public String getTarget() { return target; }
	public void setTarget(String target) { this.target = target; }
	
	private long created = System.currentTimeMillis(); 
	public long getCreated() { return created; }
	public void setCreated(long created) { this.created = created; }
	public String getCreatedString() { return Utils.dateString(created); }

	private long started;  
	public long getStarted() { return started; }
	public void setStarted(long started) { this.started = started; }
	public String getStartedString() { return Utils.dateString(started); }

	private long now;
	public long getNow() { return now; }
	public void setNow(long now) { this.now = now; }

	private long ended;
	public long getEnded() { return ended; }
	public void setEnded(long ended) { this.ended = ended; }
	public String getEndedString() { return Utils.dateString(ended); }

	private long totalDataSize; // file size, 0 if unknown
	public long getTotalDataSize() { return totalDataSize; }
	public void setTotalDataSize(long size) { this.totalDataSize = size; }

	private long bytesTransferred = 0l; 
	public long getBytesTransferred() {	return bytesTransferred; }
	public void setBytesTransferred(long bytesTransferred) { this.bytesTransferred = bytesTransferred; }

	private String failureCause; // cause if FAILED/CANCELED/ABORTED
	public String getFailureCause() { return failureCause; }
	public void setFailureCause(String failureCause) { this.failureCause = failureCause; }
}