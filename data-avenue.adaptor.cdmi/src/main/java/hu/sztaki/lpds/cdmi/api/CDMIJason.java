package hu.sztaki.lpds.cdmi.api;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static hu.sztaki.lpds.cdmi.api.CDMIConstants.*;

public class CDMIJason {

	public static Map<String, Object> getMetadataMap(InputStreamReader is) throws ParseException {
		JSONParser parser = new JSONParser();
		try {
			@SuppressWarnings("unchecked")
			Map<String,Object> map = (Map<String,Object>) parser.parse(is);
			if (map == null || !map.containsKey(CDMI_METADATA) || map.get(CDMI_METADATA) == null) throw new ParseException(0, "No metadata! (No such key or null value.)");
			@SuppressWarnings("unchecked")
			Map<String,Object> metadataMap = (Map<String,Object>) map.get(CDMI_METADATA);
			return metadataMap;
		} catch (IOException e) { throw new ParseException(0, "IOException during parsing JSON"); }
	}
	
	public static List <String> getChildren(InputStreamReader is) throws ParseException {
		JSONParser parser = new JSONParser();
		try {
			@SuppressWarnings("unchecked")
			Map<String,Object> map = (Map<String,Object>) parser.parse(is);
			if (map == null || !map.containsKey(CDMI_CHILDREN) || map.get(CDMI_CHILDREN) == null) throw new ParseException(0, "No children! (No such key or null value.)"); 
			@SuppressWarnings("unchecked")
			List<String> children = (List<String>) map.get(CDMI_CHILDREN);
	        return children;
		} catch (IOException e) { throw new ParseException(0, "IOException during parsing JSON!"); }
	}
}
