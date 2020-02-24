package hu.sztaki.lpds.dataavenue.core;

import fr.in2p3.jsaga.adaptor.security.VOMSContext;
import hu.sztaki.lpds.dataavenue.core.interfaces.exceptions.NotSupportedProtocolException;
import hu.sztaki.lpds.dataavenue.core.interfaces.impl.CredentialsImpl;
import hu.sztaki.lpds.dataavenue.core.interfaces.impl.URIFactory;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;

import java.util.Map;
import java.util.UUID;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.MapKeyColumn;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.JoinColumn;
import javax.persistence.Transient;

import org.ogf.saga.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class represents a persistable alias information that holds all necessary data (including credentials) required to access a resource. 
 */
@NamedQueries({
@NamedQuery(
	    name="HttpAlias.findAll",
	    query="SELECT i FROM HttpAlias AS i ORDER BY i.created DESC"
),
@NamedQuery(
	    name="HttpAlias.findExpired",
	    query="SELECT i FROM HttpAlias AS i WHERE i.expires > 0 AND i.expires < :p"
),
@NamedQuery(
	    name="HttpAlias.deleteExpired", /* this does not delete credentials children from aliasCredentials table */
	    query="DELETE FROM HttpAlias i WHERE i.expires > 0 AND i.expires < :p"
)
})

@Deprecated
@Entity
public class HttpAlias {
	@Transient private static final Logger log = LoggerFactory.getLogger(HttpAlias.class);
	@Transient private static final String [] credKeysToEncrypt = new String [] {Context.USERCERT, Context.USERKEY, Context.USERPASS, Context.USERPROXY, VOMSContext.INITIALPROXY, "UserPrivateKey" };
	
	public HttpAlias() {} // default constructor for jpa
	
	HttpAlias(final String ticket, final URIBase uri, final Credentials credentials, final DataAvenueSession session, final boolean isRead, final int lifetime, final boolean extractArchive, final String directURL) throws NotSupportedProtocolException, URIException, CredentialException, OperationException {
		setTicket(ticket);
		setSource(uri.getURI());
		setCredentials(credentials != null ? credentials.getCredentialsMap() : null);
		setExtractArchive(extractArchive);
		setForReading(isRead);
		setExpires(created + (lifetime > 0 ? lifetime * 1000l : Configuration.DEFAULT_ALIAS_EXPIRATION_TIME * 1000l));
		setDirectURL(directURL);
	}

	@Id
	private String aliasId = UUID.randomUUID().toString();
	public void setAliasId(String aliasId) { this.aliasId = aliasId; }
	public String getAliasId() { return aliasId; }

	private long created = System.currentTimeMillis();
	public void setCreated(long creationTime) { this.created = creationTime;	}
	public long getCreated() { return created; }
	public String getCreatedString() { return Utils.dateString(created); }

	private long expires = 0l;
	public void setExpires(long expires) { this.expires = expires;	}
	public long getExpires() { return expires; }
	public String getExpiresString() { return Utils.dateString(expires); }
	
	public enum HttpAliasStatus { CREATED, TRANSFERRING, DONE, FAILED } 
	private HttpAliasStatus status = HttpAliasStatus.CREATED;
	public HttpAliasStatus getStatus() { return status; }
	public void setStatus(HttpAliasStatus status) {
		switch (status) {
			case DONE:
			case FAILED:
				if (progress > 0l) Statistics.incBytesTransferred(progress); // increment bytes transferred statistics
				break;
			default: // do nothing
		}
		this.status = status;
	}
	
	private String ticket;
	public void setTicket(String ticket) { this.ticket = ticket; }
	public String getTicket() { return ticket; }

	@Lob
	private String source;
	public void setSource(String source) { this.source = source; }
	public String getSource() { return source; }
	public URIBase getUri() throws URIException { return URIFactory.createURI(source); }
	public String getFileName() throws URIException { return URIFactory.createURI(source).getEntryName(); }
	
	@ElementCollection(fetch = FetchType.EAGER)
	@MapKeyColumn(name="attributeName", columnDefinition="VARCHAR(255) NOT NULL") // key is a reserved SQL keyword, @Lob may apply to attribute name resulting in: BLOB/TEXT column 'attributeName' used in key specification without a key length
	@Lob @Column(name="attributeValue")
	@CollectionTable(name="aliasCredentials", joinColumns=@JoinColumn(name="aliasId"))
	private Map<String,String> credentials = null; // JPA does not like interfaces, use Map directly
	public void setCredentials(Map<String,String> credentials) { this.credentials = credentials; }
	public Credentials getCredentials() { return credentials != null && credentials.size() > 0 ? new CredentialsImpl(credentials) : null; } 
	
	private boolean isForReading;
	public void setForReading(boolean isRead) { this.isForReading = isRead;	}
	public boolean isForReading() { return isForReading; }

	private boolean extractArchive;
	public void setExtractArchive(boolean extractArchive) { this.extractArchive = extractArchive;	}
	public boolean extractArchive() { return extractArchive; }
	
	private String directURL; // if directURL could be created, null otherwise
	public String getDirectURL() { return directURL; }
	public void setDirectURL(String directURL) { this.directURL = directURL; }
	
	private long progress; // bytes transferred 
	public long getProgress() { return progress; }
	public void setProgress(long bytesTransferred) { this.progress = bytesTransferred; }
	
	public String failureReason; // if FAILED
	public String getFailureReason() { return failureReason; }
	public void setFailureReason(String failureReason) { this.failureReason = DBManager.abbreviate(failureReason); }

	private long size; // obtain on read mode only
	public long getSize() { return size; }
	public void setSize(long size) { 
		if (size <= 0l) return;
		this.size = size; 
	}

	public void transferring(final long bytesTransferred, final long fileSize) {
		if (HttpAliasRegistry.UPDATE_ALIAS_STATE) {
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			if (entityManager != null) {
				entityManager.getTransaction().begin();
				HttpAlias alias = entityManager.find(HttpAlias.class, getAliasId());
				if (alias != null) {
					alias.setProgress(bytesTransferred);
					alias.setSize(fileSize);
					alias.setStatus(HttpAliasStatus.TRANSFERRING);
				} else log.debug("Alias removed (expired), TRANSFERRING state not registered");
				entityManager.getTransaction().commit();
				entityManager.close();
			}  
		} else {
			// do nothing
		}
	}
	
	public void done(final long bytesTransferred, final long fileSize) {
		if (HttpAliasRegistry.UPDATE_ALIAS_STATE) {
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			if (entityManager != null) {
				entityManager.getTransaction().begin();
				HttpAlias alias = entityManager.find(HttpAlias.class, getAliasId());
				if (alias != null) {
					alias.setProgress(bytesTransferred);
					alias.setSize(fileSize);
					alias.setStatus(HttpAliasStatus.DONE);
				} else log.debug("Alias removed (expired), DONE state not registered");
				entityManager.getTransaction().commit();
				entityManager.close();
			}
		} else {
			if (bytesTransferred > 0l) Statistics.incBytesTransferred(bytesTransferred);
		}
	}

	public void failed(final String reason, final long bytesTransferred, final long fileSize) {
		if (HttpAliasRegistry.UPDATE_ALIAS_STATE) {
			EntityManager entityManager = DBManager.getInstance().getEntityManager();
			if (entityManager != null) {
				entityManager.getTransaction().begin();
				HttpAlias alias = entityManager.find(HttpAlias.class, getAliasId());
				if (alias != null) {
					alias.setFailureReason(reason);
					alias.setProgress(bytesTransferred);
					alias.setSize(fileSize);
					alias.setStatus(HttpAliasStatus.FAILED);
				} else log.debug("Alias removed (expired), FAILED state not registered");
				entityManager.getTransaction().commit();
				entityManager.close();
			}
		} else {
			if (bytesTransferred > 0l) Statistics.incBytesTransferred(bytesTransferred);
		}
	}
	
	// encode sensitive credential attributes
	void encode() {
		Map<String,String> map = this.credentials;
		if (map == null) return;
		HttpAliasCredentialsEncoder encoder = HttpAliasCredentialsEncoder.getInstance();
		for (String key: credKeysToEncrypt) {
			String value = map.get(key);
			if (value != null) {
				log.trace("Encoding credential attribute: " + key);
				map.put(key, encoder.encrypt(value));
			}
		}
	}

	// decode sensitive credential attributes
	void decode() throws Exception {
		Map<String,String> map = this.credentials;
		if (map == null) return;
		HttpAliasCredentialsEncoder decoder = HttpAliasCredentialsEncoder.getInstance();
		for (String key: credKeysToEncrypt) {
			String value = map.get(key);
			if (value != null) {
				log.trace("Decoding credential attribute: " + key);
				map.put(key, decoder.decrypt(value));
			}
		}
	}
}