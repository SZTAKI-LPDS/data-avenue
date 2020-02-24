/**
 * 
 */
package hu.sztaki.lpds.dataavenue.core.interfaces.impl;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

/**
 *
 */
public class FileEntryImpl extends DirEntryImpl {

	protected Long size; // n.a.
	
	public FileEntryImpl(final String url) throws URIException {
		super(url);
	}

	public FileEntryImpl(final String url, final Long size, final Long lastModified, final Boolean readPermisssion, final Boolean writePermission, final Boolean executePermission) throws URIException {
		this(url);
		this.size = size;
		this.lastModified = lastModified;
		this.setOwnerReadPermission(readPermisssion);
		this.setOwnerWritePermission(writePermission);
		this.setOwnerExecutePermission(executePermission);
	}
	
	@Override
	public URIType getType() {
		return URIType.FILE;
	}

	@Override
	public Long getSize() {
		return size; 
	}
	
	public void setSize(final long size) {
		this.size = size;
	}

	@Override public String getPermissions() {
		return "-" + getPermissionsWithoutType();
	}
	
	@Override
	public String toString() {
		String sizeString = size != null ? "" + size : "?";
		return super.toString() + " " + sizeString + "B ";
	}
}