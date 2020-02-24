package hu.sztaki.lpds.dataavenue.interfaces.impl;

import java.util.List;
import java.util.Vector;

import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationField;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationType;

public class AuthenticationTypeImpl implements AuthenticationType {
	
	private List<AuthenticationField> fields = new Vector <AuthenticationField>();
	private String keyName = "", displayName = "";
	
	@Override
	public String getType() {
		return keyName;
	}
	
	@Override
	public void setType(String param) {
		this.keyName = param;
	}
	
	@Override
	public String getDisplayName() {
		return displayName;
	}

	@Override
	public void setDisplayName(String param) {
		this.displayName = param;
	}
	
	@Override
	public List<AuthenticationField> getFields() {
		return fields;
	}

	@Override
	public void setFields(List<AuthenticationField> param) {
		this.fields = param;
	}
}
