package hu.sztaki.lpds.dataavenue.interfaces;

import java.util.List;

public interface AuthenticationType {
	public String getType();
	public void setType(String param);
	public String getDisplayName();
	public void setDisplayName(String param);
	public List<AuthenticationField> getFields();
	public void setFields(List<AuthenticationField> param);
}
