package hu.sztaki.lpds.dataavenue.core.webservices;

import hu.sztaki.lpds.dataavenue.core.interfaces.impl.CredentialsImpl;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;

import java.util.Iterator;
import java.util.List;

import org.ogf.saga.context.Context;

@Deprecated
public class CredentialAttributes {
	
	public static final String CERTREPOSITORY = Context.CERTREPOSITORY; // JSaga constant  
	public static final String LIFETIME = Context.LIFETIME; // JSaga constant
	public static final String REMOTEHOST = Context.REMOTEHOST; // JSaga constant
	public static final String REMOTEID = Context.REMOTEID; // JSaga constant
	public static final String REMOTEPORT = Context.REMOTEPORT; // JSaga constant
	public static final String SERVER = Context.SERVER; // JSaga constant
	public static final String TYPE = Context.TYPE; // JSaga constant
	public static final String USERCERT = Context.USERCERT; // JSaga constant
	public static final String USERID = Context.USERID; // JSaga constant
	public static final String USERKEY = Context.USERKEY; // JSaga constant
	public static final String USERPASS = Context.USERPASS; // JSaga constant
	public static final String USERPROXY = Context.USERPROXY; // JSaga constant
	public static final String USERVO = Context.USERVO; // JSaga constant
	
	private String certRepository;
	private String lifeTime;
	private String remoteHost;
	private String remoteID;
	private String remotePort;
	private String server;
	private String type;
	private String userCert;
	private String userID;
	private String userKey;
	private String userPass;
	private String userProxy;
	private String userVO;
	private List<CredentialAttribute> otherCredentialAttributes;

	public String getCertRepository() {
		return certRepository;
	}

	public void setCertRepository(String certrepository) {
		this.certRepository = certrepository;
	}

	public String getLifeTime() {
		return lifeTime;
	}

	public void setLifeTime(String lifetime) {
		this.lifeTime = lifetime;
	}

	public String getRemoteHost() {
		return remoteHost;
	}

	public void setRemoteHost(String remotehost) {
		this.remoteHost = remotehost;
	}

	public String getRemoteID() {
		return remoteID;
	}

	public void setRemoteID(String remoteID) {
		this.remoteID = remoteID;
	}

	public String getRemotePort() {
		return remotePort;
	}

	public void setRemotePort(String remotePort) {
		this.remotePort = remotePort;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getUserCert() {
		return userCert;
	}

	public void setUserCert(String userCert) {
		this.userCert = userCert;
	}

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public String getUserKey() {
		return userKey;
	}

	public void setUserKey(String userKey) {
		this.userKey = userKey;
	}

	public String getUserPass() {
		return userPass;
	}

	public void setUserPass(String userPass) {
		this.userPass = userPass;
	}

	public String getUserProxy() {
		return userProxy;
	}

	public void setUserProxy(String userProxy) {
		this.userProxy = userProxy;
	}

	public String getUserVO() {
		return userVO;
	}

	public void setUserVO(String UserVO) {
		this.userVO = UserVO;
	}

	public List<CredentialAttribute> getOtherCredentialAttributes() {
		return otherCredentialAttributes;
	}

	public void setOtherCredentialAttributes(
			List<CredentialAttribute> otherCredentialAttributes) {
		this.otherCredentialAttributes = otherCredentialAttributes;
	}
	
    // creates credentials map (IF) from CredentialAttributes
    public Credentials getCredentials() {
        CredentialsImpl credentials = new CredentialsImpl();
        if (getCertRepository() != null) credentials.putCredentialAttribute(CERTREPOSITORY, getCertRepository());
        if (getLifeTime() != null) credentials.putCredentialAttribute(LIFETIME, getLifeTime());
        if (getRemoteHost() != null) credentials.putCredentialAttribute(REMOTEHOST, getRemoteHost());
        if (getRemoteID() != null) credentials.putCredentialAttribute(REMOTEID, getRemoteID());
        if (getRemotePort() != null) credentials.putCredentialAttribute(REMOTEPORT, getRemotePort());
        if (getServer() != null) credentials.putCredentialAttribute(SERVER, getServer());
        if (getType() != null) credentials.putCredentialAttribute(TYPE, getType());
        if (getUserCert() != null) credentials.putCredentialAttribute(USERCERT, getUserCert());
        if (getUserID() != null) credentials.putCredentialAttribute(USERID, getUserID());
        if (getUserKey() != null) credentials.putCredentialAttribute(USERKEY, getUserKey());
        if (getUserPass() != null) credentials.putCredentialAttribute(USERPASS, getUserPass());
        if (getUserProxy() != null) credentials.putCredentialAttribute(USERPROXY, getUserProxy());
        if (getUserVO() != null) credentials.putCredentialAttribute(USERVO, getUserVO());
        if (getOtherCredentialAttributes() != null) {
	        Iterator<CredentialAttribute> i = getOtherCredentialAttributes().iterator();
	        while (i.hasNext()) {
	            CredentialAttribute c = (CredentialAttribute) i.next();
	            credentials.putCredentialAttribute(c.getKey(), c.getValue());
	        }
        }
        return credentials;
    }
	
	@Override public String toString() {
		StringBuilder others = new StringBuilder();
		if (otherCredentialAttributes != null) for (CredentialAttribute other: otherCredentialAttributes) others.append(other.getKey() + " ");
		String attrs =
				(userProxy != null ? "userProxy " : "") +
				(certRepository != null ? "certRepository " : "") +
				(lifeTime != null ? "lifeTime " : "") +
				(remoteHost != null ? "remoteHost " : "") +
				(remoteID != null ? "remoteID " : "") +
				(remotePort != null ? "remotePort " : "") +
				(server != null ? "server " : "") +
				(type != null ? "type " : "") +
				(userCert != null ? "userCert " : "" ) +
				(userID != null ? "userID " : "") +
				(userKey != null ? "userKey " : "") +
				(userPass != null ? "userPass " : "") +
				(userVO != null ? "userVO " : "") +
				(lifeTime != null ? "lifeTime " : "") +
				(otherCredentialAttributes != null ? others.toString() : "");
		if (attrs.endsWith(" ")) attrs = attrs.substring(0, attrs.length() - 1);
		return "[" + attrs + "]";
	}
}