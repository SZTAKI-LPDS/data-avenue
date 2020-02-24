package hu.sztaki.lpds.dataavenue.core.interfaces.impl;

import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;


public class SymLinkEntryImpl extends FileEntryImpl {

	public SymLinkEntryImpl(final String url) throws URIException {
		super(url);
	}
	
	@Override
	public URIType getType() {
		return URIType.SYMBOLIC_LINK;
	}

	@Override public String getPermissions() {
		return "l" + getPermissionsWithoutType();
	}
}
