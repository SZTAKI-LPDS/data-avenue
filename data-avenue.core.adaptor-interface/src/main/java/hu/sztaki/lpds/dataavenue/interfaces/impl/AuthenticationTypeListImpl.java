package hu.sztaki.lpds.dataavenue.interfaces.impl;

import java.util.List;
import java.util.Vector;

import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationType;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationTypeList;

public class AuthenticationTypeListImpl implements AuthenticationTypeList {

	private List<AuthenticationType> authenticationTypes = new Vector<AuthenticationType> ();
	
	@Override
	public List<AuthenticationType> getAuthenticationTypes() {
		return authenticationTypes;
	}

	@Override
	public void setAuthenticationTypes(List<AuthenticationType> param) {
		this.authenticationTypes = param;
	}
}