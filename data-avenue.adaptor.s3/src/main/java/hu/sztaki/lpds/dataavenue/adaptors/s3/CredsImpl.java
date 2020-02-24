package hu.sztaki.lpds.dataavenue.adaptors.s3;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;

public final class CredsImpl implements Credentials {
	private final Map<String, String> map;
	public CredsImpl() { map = new Hashtable<String, String>(); }
	public CredsImpl(final Map<String, String> map) {	this.map = map != null ? new Hashtable<String, String>(map) : null; }

	@Override public String getCredentialAttribute(final String key) { return map != null ? map.get(key) : null; }
	@Override public String optCredentialAttribute(final String key, final String defaultValue) { 
		if (map == null || map.get(key) == null || "".equals(map.get(key))) return defaultValue;
		else return map.get(key); 
	}
	@Override public void putCredentialAttribute(final String key, final String value) { if (map != null) map.put(key, value); }
	@Override public Set<String> keySet() { return map != null ? map.keySet() : null; }
	@Override public Map<String, String> getCredentialsMap() { return this.map; }
}