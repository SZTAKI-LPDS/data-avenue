package hu.sztaki.lpds.dataavenue.core.webservices;

import hu.sztaki.lpds.dataavenue.interfaces.URIBase;

@Deprecated
public class DirEntry {
	
	public DirEntry() {} // required by com.sun.xml.bind.v2.runtime
	
	public DirEntry(URIBase uri) {
		setType(uri.getType());
		setName(uri.getEntryName());
		setSize(uri.getSize());
		setLastModified(uri.getLastModified());
		setPermissions(uri.getPermissions());
	}

	private URIBase.URIType type;
	public void setType(URIBase.URIType type) { this.type = type; }
	public URIBase.URIType getType() { return this.type; }
	
	private String name;
	public String getName() { return name; }
	public void setName(String name) { this.name = name; }
	
	private Long size;
	public Long getSize() {	return size; }
	public void setSize(Long size) { this.size = size; }

	private Long lastModified;
	public Long getLastModified() {	return lastModified; }
	public void setLastModified(Long lastModified) { this.lastModified = lastModified;	}
	
	private String permissions; // UNIX style permissions string 
	public String getPermissions() { return permissions; }
	public void setPermissions(String permissions) { this.permissions = permissions; }
}