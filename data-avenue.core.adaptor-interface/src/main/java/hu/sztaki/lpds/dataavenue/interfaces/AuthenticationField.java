package hu.sztaki.lpds.dataavenue.interfaces;

public interface AuthenticationField {
	public static final String TEXT_TYPE = "text", PASSWORD_TYPE = "password"; 
	public String getKeyName();
	public void setKeyName(String param);
	public String getDisplayName();
	public void setDisplayName(String param);
	public String getDefaultValue();
	public void setDefaultValue(String param);
	public String getType();
	public void setType(String param);
}
