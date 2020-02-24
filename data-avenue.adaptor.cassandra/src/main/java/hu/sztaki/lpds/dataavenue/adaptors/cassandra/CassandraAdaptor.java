package hu.sztaki.lpds.dataavenue.adaptors.cassandra;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationField;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationType;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationTypeList;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum;
import hu.sztaki.lpds.dataavenue.interfaces.TransferMonitor;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationNotSupportedException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationFieldImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationTypeImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationTypeListImpl;

import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.*;

import org.apache.commons.codec.binary.Base64;


public class CassandraAdaptor implements Adaptor {
	
	private static final Logger log = LoggerFactory.getLogger(CassandraAdaptor.class);
	
	private String adaptorVersion = "1.0.0"; // default adaptor version
	
	public static final String CASSANDRA_PROTOCOL = "cassandra"; // cassandra://
	public static final String CASSANDRA_SESSION = "cassandra"; 

	static final String NONE_AUTH = "None";
	static final String USERPASS_AUTH = "UserPass"; 
	
	public CassandraAdaptor() {
		String PROPERTIES_FILE_NAME = "META-INF/data-avenue-adaptor.properties"; // try to read version number
		try {
			Properties prop = new Properties();
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME); // emits no exception but null returned
			if (in == null) log.warn("Cannot find properties file: " + PROPERTIES_FILE_NAME); 
			else {
				try {
					prop.load(in);
					try { in.close(); } catch (IOException e) {}
					if (prop.get("version") != null) adaptorVersion = (String) prop.get("version");
				} catch (Exception e) { log.warn("Cannot load properties from file: " + PROPERTIES_FILE_NAME); }
			}
		} catch (Throwable e) { log.warn("Cannot read properties file: " + PROPERTIES_FILE_NAME); } 
	}
	
	/* adaptor meta information */
	@Override public String getName() { return "Cassandra Adaptor"; }
	@Override public String getDescription() { return "Cassandra Adaptor allows of connecting to Cassandra databases via Data Avenue"; }
	@Override public String getVersion() { return adaptorVersion; }
	
	@Override  public List<String> getSupportedProtocols() {
		List<String> result = new Vector<String>();
		result.add(CASSANDRA_PROTOCOL);
		return result;
	}
	
	@Override public List<OperationsEnum> getSupportedOperationTypes(String protocol) {
		List<OperationsEnum> result = new Vector<OperationsEnum>();
		result.add(LIST); 
		result.add(INPUT_STREAM);  
		return result;
	}
	
	@Override public List<OperationsEnum> getSupportedOperationTypes(final URIBase fromURI, final URIBase toURI) {
		return Collections.<OperationsEnum>emptyList();
	}
	
	@Override public List<String> getAuthenticationTypes(String protocol) {
		List<String> result = new Vector<String>();
		if (CASSANDRA_PROTOCOL.equals(protocol)) {
			result.add(NONE_AUTH);
			result.add(USERPASS_AUTH);
		}
		return result;
	}
	

	@Override
	public AuthenticationTypeList getAuthenticationTypeList(String protocol) {
		
		AuthenticationTypeList l = new AuthenticationTypeListImpl();

		AuthenticationType a = new AuthenticationTypeImpl();
		a.setType(NONE_AUTH);
		a.setDisplayName("No authentication");

		l.getAuthenticationTypes().add(a);
		
		a = new AuthenticationTypeImpl();
		a.setType(USERPASS_AUTH);
		a.setDisplayName("Cassandra authentication");
		
		AuthenticationField f1 = new AuthenticationFieldImpl();
		f1.setKeyName("UserID"); // "UserID"
		f1.setDisplayName("Username");
		a.getFields().add(f1);
		
		AuthenticationField f2 = new AuthenticationFieldImpl();
		f2.setKeyName("UserPass"); // "UserPass"
		f2.setDisplayName("Password");
		f2.setType(AuthenticationField.PASSWORD_TYPE);
		a.getFields().add(f2);
		
		l.getAuthenticationTypes().add(a);
		
		return l;
	}
	
	@Override public String getAuthenticationTypeUsage(String protocol,
			String authenticationType) {
		if (protocol == null || authenticationType == null) throw new IllegalArgumentException("null argument");
		if (NONE_AUTH.equals(authenticationType)) return "(No credentials required.)";
		if (USERPASS_AUTH.equals(authenticationType)) return "<b>UserID</b> (access key), <b>UserPass</b> (secret key)";
		return null;
	}
	
	private String escape(String s) { // table names with mixed upper/lowercase chars need escaping
		return "\"" + s + "\"";  //String.format("\"%s\"", s)*/
	}
	
	@Override public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}
	
	@Override public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession session, List <String> subentires) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}
	
	@Override public List<URIBase> list(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		List<URIBase> result = new Vector<URIBase>();
		
		Session conn = getCassandraSession(uri, credentials, session);
		CassandraURI cURI = new CassandraURI(uri);
		
		if (cURI.getKeyspace() == null) { // list keyspaces
			log.debug("Listing keyspaces...");
			List<KeyspaceMetadata> keyspaces = conn.getCluster().getMetadata().getKeyspaces();
			for (KeyspaceMetadata keyspaceMetadata: keyspaces) {
				String keyspace = keyspaceMetadata.getName();
				// keep query string and fragment 
				CassandraURI entry = cURI.withNewPath(cURI.getPath().endsWith(CassandraURI.PATH_SEPARATOR) ? 
														cURI.getPath() + keyspace + CassandraURI.PATH_SEPARATOR :
														cURI.getPath() + CassandraURI.PATH_SEPARATOR + keyspace + CassandraURI.PATH_SEPARATOR
									);
				entry.setSize((long)keyspaceMetadata.getTables().size());
				entry.setSizeUnit("tables"); // show the number of tables
				entry.setLastModified(System.currentTimeMillis());
				result.add(entry);
			}
			
		} else if (cURI.getTable() == null){ // list tables
			log.debug("Listing tables...");
			KeyspaceMetadata keyspaceMetadata = conn.getCluster().getMetadata().getKeyspace(cURI.getKeyspace());
			if (keyspaceMetadata != null) {
				for (TableMetadata tableMetadata: keyspaceMetadata.getTables()) {
					String tableName = tableMetadata.getName();
					// keep query string and fragment 
					CassandraURI entry = cURI.withNewPath(cURI.getPath().endsWith(CassandraURI.PATH_SEPARATOR) ? 
							cURI.getPath() + tableName :
							cURI.getPath() + CassandraURI.PATH_SEPARATOR + tableName
							);
					
					long size = 0l;
					try {
						Statement statement = QueryBuilder.select().countAll().from(escape(cURI.getKeyspace()), escape(tableMetadata.getName())); 
						log.debug(statement.toString());
						ResultSet results = conn.execute(statement);
						Row row = results.one();
						size = row.getLong(0); // count column
						entry.setSize(size);
					} catch (Exception x) { log.warn("Query exception at table " + tableMetadata.getName(), x); }
					entry.setSizeUnit("rows"); // show the number of rows
					entry.setLastModified(System.currentTimeMillis());
					result.add(entry);
				}
			}
		} else {
			throw new OperationException("Listing table contents is illegal!");
		}
		return result;
	}

	@Override public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public void delete(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}


	@Override public void permissions(final URIBase uri, final Credentials credentials, final DataAvenueSession session, final String permissionsString) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}
	
	@Override public void rename(URIBase uri, String newName, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@SuppressWarnings("unchecked")
	@Override public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		
		Session conn = getCassandraSession(uri, credentials, session);
		CassandraURI cURI = new CassandraURI(uri);
		
		if (cURI.getKeyspace() == null) throw new URIException("No keyspace provided!");
		if (cURI.getTable() == null) throw new URIException("No tablename provided!");

		// create query
		Select select = QueryBuilder.select().all().from(escape(cURI.getKeyspace()), escape(cURI.getTable()));

		// set fetch size
		int fetchSize = Integer.MAX_VALUE; // unlimited
		if (cURI.getFragment() != null) { // set fetch size if set as fragment, e.g. #100 (0: default fetch size)
			try { fetchSize = Integer.parseInt(cURI.getFragment()); } catch(Exception x) {}
			if (fetchSize < 0) fetchSize = Integer.MAX_VALUE; // invalid value
		}
		select.setFetchSize(fetchSize);
		
		// set where clause
		Map<String, List<String>> queryMap = null;
		if (cURI.getQuery() != null) { 
			try { 
				queryMap = splitQuery(cURI.getQuery());
				for (String key: queryMap.keySet()) {
					if (queryMap.get(key) == null || queryMap.get(key).size() == 0) continue;
					if (queryMap.get(key).size() == 1) { // equals clause
						select.where().and(QueryBuilder.eq(key, queryMap.get(key).get(0)));
					} else { // in clause
						select.where().and(QueryBuilder.in(key, queryMap.get(key).toArray()));
					}
				}
			} catch(Exception x) { log.warn("Cannot parse query string", x); }
		}
		
		// set order by setOrderBy
//		if (queryMap != null && queryMap.containsKey("orderby")) {
//			for (String col: queryMap.get("orderby")) {
//				select.orderBy(new Ordering(col, false)); // constructor Ordering not visible!
//			}
//		}
		
		log.debug("query: " + select);
		
		ResultSet results = null;
		try { results = conn.execute(select); }
		catch (NoHostAvailableException x) { throw new OperationException("No host in the cluster can be contacted successfully to execute this query!", x); }
		catch (QueryExecutionException x) { throw new OperationException("Cassandra cannot execute the query with the requested consistency level successfully!", x); }
		catch (QueryValidationException x) { throw new OperationException("The query if invalid (syntax error, unauthorized or any other validation problem)!", x); }
		catch (UnsupportedFeatureException x) { throw new OperationException("Protocol version 1 is in use and a feature not supported has been used!", x); } 

		ColumnDefinitions colDefs = results.getColumnDefinitions();
		
		int numberOfColumns = colDefs.size();
		log.debug("number of columns: " + numberOfColumns);
		
		JSONArray rowsJSON = new JSONArray();

		Iterator<Row> it = results.iterator();
		while (it.hasNext()) {
			Row row = it.next();
			JSONObject mapJSON = new JSONObject();

			for (int i = 0; i < numberOfColumns; i++) {
				cassandra2json(mapJSON, i, colDefs, row);
			}
			rowsJSON.add(mapJSON);
		}

		// TODO create an inputstream wrapper (read rows from db as input stream is read to avoid large memory allocation)
//		return new ByteArrayInputStream(rowsJSON.toJSONString().getBytes());
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create(); 
		String jsonOutput = gson.toJson(rowsJSON);
		return new ByteArrayInputStream(jsonOutput.getBytes());
	}
	
	@SuppressWarnings({"unchecked"})
	private void cassandra2json(JSONObject container, String colName, DataType t, Row row, int index) {
		switch (t.getName()) { 
			case BOOLEAN:
				container.put(colName, new Boolean(row.getBool(index)));
				break;
			case INT:
				container.put(colName, new Integer(row.getInt(index)));
				break;
			case DOUBLE:
				container.put(colName, new Double(row.getDouble(index)));
				break;
			case FLOAT:
				container.put(colName, new Double(row.getFloat(index)));
				break;
			case ASCII:
			case TEXT:
			case VARCHAR:
				container.put(colName, row.getString(index));
				break;
			case BIGINT:
			case COUNTER:
				container.put(colName, "" + row.getLong(index)); 
				break;
			case DECIMAL:
				container.put(colName, (row.getDecimal(index) != null ? row.getDecimal(index).toString() : null));
				break;
			case VARINT:
				container.put(colName, (row.getVarint(index) != null ? row.getVarint(index).toString() : null));
				break;
			case BLOB:
			case CUSTOM:
				container.put(colName, (row.getBytes(index) != null ? new String(row.getBytesUnsafe(index).array()) : null));
				break;
			case INET:
				container.put(colName, (row.getInet(index) != null ? row.getInet(index).toString() : null));
				break;
			case TIMESTAMP:
				container.put(colName, (row.getDate(index) != null ? row.getDate(index).toString() : null));
				break;
			case TIMEUUID:
			case UUID:
				container.put(colName, (row.getUUID(index) != null ? row.getUUID(index).toString() : null));
				break;
			default: // should not happen	
				container.put(colName, (row.getBytesUnsafe(index) != null ? new String(row.getBytesUnsafe(index).array()) : null));
		}
	}
	
	@SuppressWarnings("unchecked")
	private void add2jsonArray(JSONArray container, DataType t, Object o) {
		container.add(toJsonObject(t, o));
	}
	
	Object toJsonObject(DataType t, Object o) {
		switch (t.getName()) {
			case BOOLEAN: // Boolean
				return new Boolean((Boolean)o);
			case INT: // Integer
				return new Integer((Integer)o);
			case DOUBLE: // Double 
				return new Double((Double)o);
			case FLOAT: // Float
				return new Double((Float)o);
			case ASCII: // String
			case TEXT:
			case VARCHAR:
				return new String((String)o);
			case BLOB: // base64 encode
			case CUSTOM:
				if (o != null) {
					try { return Base64.encodeBase64(((ByteBuffer)o).array()); } 
					catch (Exception x) { log.warn("UnsupportedEncodingException", x);}
				}
				return null;	
			default: 	
				return o != null ? new String(o.toString()) : null; 
		}
	}
	
	@SuppressWarnings("unchecked")
	private void add2jsonMap(JSONObject container, DataType kt, Object k, DataType vt, Object v) {
		container.put(toJsonObject(kt, k), toJsonObject(vt, v));
	}
	
	@SuppressWarnings("unchecked")
	private void cassandra2json(JSONObject container, int index, ColumnDefinitions colDefs, Row row) {
		
		if (!colDefs.getType(index).isCollection()) { // primitive
			
			cassandra2json(container, colDefs.getName(index), colDefs.getType(index), row, index);
			
		} else { // list, set, map
			
			switch (colDefs.getType(index).getName()) {
				case LIST:
					try {
						DataType t = colDefs.getType(index).getTypeArguments().get(0); // returns data type of the list
						JSONArray jsonArray = new JSONArray();
						for (Object o: row.getList(index, t.asJavaClass())) 
							add2jsonArray(jsonArray, t, o);
						container.put(colDefs.getName(index), jsonArray);
					} catch (Exception x) { log.warn("Cannot process list-type field: {} ", colDefs.getName(index), x); }
					break;
				case SET:
					try {
						DataType t = colDefs.getType(index).getTypeArguments().get(0); // returns data type of the list
						JSONArray jsonArray = new JSONArray();
						for (Object o: row.getSet(index, t.asJavaClass())) 
							add2jsonArray(jsonArray, t, o);
						container.put(colDefs.getName(index), jsonArray);
					} catch (Exception x) { log.warn("Cannot process set-type field: {} ", colDefs.getName(index), x); }
					break;
				case MAP:
					try {
						DataType keyT = colDefs.getType(index).getTypeArguments().get(0); // returns data type of the keys
						DataType valueT = colDefs.getType(index).getTypeArguments().get(1); // returns data type of the values
						JSONObject jsonMap = new JSONObject();
						Map<?,?> map = row.getMap(index, keyT.asJavaClass(), valueT.asJavaClass());
						Iterator<?> it = map.entrySet().iterator();
					    while (it.hasNext()) {
					        Map.Entry<?,?> pair = (Map.Entry<?,?>)it.next();
					        add2jsonMap(jsonMap, keyT, pair.getKey(), valueT, pair.getValue());
						}
						container.put(colDefs.getName(index), jsonMap);
					} catch (Exception x) { log.warn("Cannot process map-type field: {} ", colDefs.getName(index), x); }
					break;
				default: // UNKNOWN TYPE	
					log.warn("Unknown cassandra type: {}", colDefs.getType(index).getName());
			}
		} 
	}

	private static Map<String, List<String>> splitQuery(String queryString) throws UnsupportedEncodingException {
		Map<String, List<String>> query_pairs = new LinkedHashMap<String, List<String>>();
		String[] pairs = queryString.split("&");
		for (String pair : pairs) {
			int idx = pair.indexOf("=");
			String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
			if (!query_pairs.containsKey(key)) query_pairs.put(key, new LinkedList<String>());
			String value = idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") : null;
			query_pairs.get(key).add(value);
		}
		return query_pairs;
	}
	
	@Override public OutputStream getOutputStream(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession, long contentLength) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}
	
	@Override public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session, InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException, OperationNotSupportedException {
		throw new OperationNotSupportedException();
	}

	@Override public String copy(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public String move(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	@Override public void cancel(String id) throws TaskIdException, OperationException {
		throw new OperationException("Operation not supported!");
	}

	@Override public long getFileSize(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Operation not supported!");
	}

	// test if object is readable
	@Override public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return true;
	}

	@Override public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return false;
	}

	@Override public void shutDown() {
	}
	
	private Session getCassandraSession(final URIBase uri, final Credentials credentials, final DataAvenueSession session) throws URIException, CredentialException, OperationException {
		if (!session.containsKey(CASSANDRA_SESSION)) session.put(CASSANDRA_SESSION, new CassandraSession());
		CassandraSession cassandraSession = (CassandraSession) session.get(CASSANDRA_SESSION);
		if (cassandraSession.get(uri) == null) cassandraSession.put(uri, credentials);
		return cassandraSession.get(uri);
	}
	
	public static void main(String [] args) throws Exception {
		Cluster cluster = Cluster.builder().addContactPoint("host").build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts() ) {
     		System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
     		host.getDatacenter(), host.getAddress(), host.getRack());
		}
		cluster.close();
 	}
}