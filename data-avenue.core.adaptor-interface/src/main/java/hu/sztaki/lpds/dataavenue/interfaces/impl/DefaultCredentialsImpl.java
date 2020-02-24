package hu.sztaki.lpds.dataavenue.interfaces.impl;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import hu.sztaki.lpds.dataavenue.interfaces.Credentials;

public final class DefaultCredentialsImpl implements Credentials {
	private final Map<String, String> map;
	public DefaultCredentialsImpl() { map = new HashMap<String, String>(); }
	public DefaultCredentialsImpl(final Map<String, String> map) {	this.map = map != null ? new Hashtable<String, String>(map) : null; }

	@Override public String getCredentialAttribute(final String key) { return map != null ? map.get(key) : null; }
	@Override public String optCredentialAttribute(final String key, final String defaultValue) { 
		if (map == null || map.get(key) == null || "".equals(map.get(key)) || "undefined".equals(map.get(key))) return defaultValue;
		else return map.get(key); 
	}
	
	@Override public void putCredentialAttribute(final String key, final String value) { if (map != null) map.put(key, value); }
	@Override public Set<String> keySet() { return map != null ? map.keySet() : null; }
	@Override public Map<String, String> getCredentialsMap() { return this.map; }
	
	@Override public String toString() {
		StringBuilder sb = new StringBuilder();
		Iterator<Entry<String, String>> iter = map.entrySet().iterator();
		while (iter.hasNext()) {
		    Entry<String, String> entry = iter.next();
		    sb.append(entry.getKey());
		    sb.append('=').append('"');
		    sb.append(entry.getValue());
		    sb.append('"');
		    if (iter.hasNext()) {
		        sb.append(',').append(' ');
		    }
		}
		return sb.toString();
	}
}