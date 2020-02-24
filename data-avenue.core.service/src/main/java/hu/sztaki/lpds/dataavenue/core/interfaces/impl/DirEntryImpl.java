/**
 * 
 */
package hu.sztaki.lpds.dataavenue.core.interfaces.impl;

import java.text.SimpleDateFormat;
import java.util.Date;

import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

/**
 * @author Akos Hajnal
 *
 */
@SuppressWarnings("deprecation")
public class DirEntryImpl extends URIImpl implements URIBase {

	protected Long lastModified; // n.a.
	
	public DirEntryImpl(final String url) throws URIException {
		super(url);
	}

	public DirEntryImpl(final String url, final Long lastModified) throws URIException {
		this(url);
		this.lastModified = lastModified;
	}

	public DirEntryImpl(final String url, final Long lastModified, final Boolean readPermisssion, final Boolean writePermission, final Boolean executePermission) throws URIException {
		this(url, lastModified);
		this.setOwnerReadPermission(readPermisssion);
		this.setOwnerWritePermission(writePermission);
		this.setOwnerExecutePermission(executePermission);
	}

	@Override
	public URIType getType() {
		return URIType.DIRECTORY;
	}

	/* (non-Javadoc)
	 * @see hu.sztaki.lpds.dataavenue.interfaces.URIBase#getLastModified()
	 */
	@Override
	public Long getLastModified() {
		return lastModified; 
	}

	public void setLastModified(final Long lastModified) {
		this.lastModified = lastModified;
	}
	
	@Override
	public String toString() {
		String dateString = lastModified == null ? "?" : new SimpleDateFormat("dd-MMM-yyyy HH:mm").format(new Date(getLastModified()));
		return getEntryName() + " " + getPermissions() + " " + getType() + " " + dateString;
	}
	
	protected Boolean ownerReadPermisssion, ownerWritePermission, ownerExecutePermission;
	protected Boolean groupReadPermisssion, groupWritePermission, groupExecutePermission;
	protected Boolean othersReadPermisssion, othersWritePermission, othersExecutePermission;
	
	public void setOwnerReadPermission(final Boolean readPermisssion) {
		this.ownerReadPermisssion = readPermisssion;
	}

	public void setOwnerWritePermission(final Boolean writePermission) {
		this.ownerWritePermission = writePermission;
	}
	
	public void setOwnerExecutePermission(final Boolean executePermission) {
		this.ownerExecutePermission = executePermission;
	}

	public void setGroupReadPermission(final Boolean readPermisssion) {
		this.groupReadPermisssion = readPermisssion;
	}

	public void setGroupWritePermission(final Boolean writePermission) {
		this.groupWritePermission = writePermission;
	}
	
	public void setGroupExecutePermission(final Boolean executePermission) {
		this.groupExecutePermission = executePermission;
	}
	
	public void setOthersReadPermission(final Boolean readPermisssion) {
		this.othersReadPermisssion = readPermisssion;
	}

	public void setOthersWritePermission(final Boolean writePermission) {
		this.othersWritePermission = writePermission;
	}
	
	public void setOthersExecutePermission(final Boolean executePermission) {
		this.othersExecutePermission = executePermission;
	}
	
	@Override public String getPermissions() {
		return "d" + getPermissionsWithoutType();
	}
	
	public String getPermissionsWithoutType() {
		StringBuilder sb = new StringBuilder();
		sb.append(ownerReadPermisssion == null ? "-" : ownerReadPermisssion ? "r" : "-");
		sb.append(ownerWritePermission == null ? "-" : ownerWritePermission ? "w" : "-");
		sb.append(ownerExecutePermission == null ? "-" : ownerExecutePermission ? "x" : "-");

		sb.append(groupReadPermisssion == null ? "-" : groupReadPermisssion ? "r" : "-");
		sb.append(groupWritePermission == null ? "-" : groupWritePermission ? "w" : "-");
		sb.append(groupExecutePermission == null ? "-" : groupExecutePermission ? "x" : "-");
		
		sb.append(othersReadPermisssion == null ? "-" : othersReadPermisssion ? "r" : "-");
		sb.append(othersWritePermission == null ? "-" : othersWritePermission ? "w" : "-");
		sb.append(othersExecutePermission == null ? "-" : othersExecutePermission ? "x" : "-");

		return sb.toString();
	}
	

	@Override
	public String getQuery() {
		return getQuery();
	}

	@Override
	public String getFragment() {
		return getFragment();
	}
}
