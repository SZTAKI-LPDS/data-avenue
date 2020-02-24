package hu.sztaki.lpds.dataavenue.interfaces.impl;

import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationField;

public class AuthenticationFieldImpl implements AuthenticationField {
	private String keyName = "", displayName = "", defaultValue = "", type = TEXT_TYPE;
	
	@Override
	public String getKeyName() {
		return keyName;
	}
	
	@Override
	public void setKeyName(String param) {
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
	public String getDefaultValue() {
		return defaultValue;
	}

	@Override
	public void setDefaultValue(String param) {
		this.defaultValue = param;
	}
	
	@Override
	public String getType() {
		return type;
	}

	@Override
	public void setType(String param) {
		this.type = param;
	}
}