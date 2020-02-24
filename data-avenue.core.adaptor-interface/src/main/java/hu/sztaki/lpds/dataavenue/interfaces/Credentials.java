package hu.sztaki.lpds.dataavenue.interfaces;

import java.util.Map;
import java.util.Set;

/*
 * Map of credential attributes: attribute key -> attribute value
 */
public interface Credentials {
	public String getCredentialAttribute(String key);
	public String optCredentialAttribute(String key, String defaultValue);
	public void putCredentialAttribute(String key, String value);
	public Map<String, String> getCredentialsMap(); // return all credentials as a map of strings
	public Set<String> keySet();
}