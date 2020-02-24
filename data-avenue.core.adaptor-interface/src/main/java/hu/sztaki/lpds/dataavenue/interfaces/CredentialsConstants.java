package hu.sztaki.lpds.dataavenue.interfaces;

public class CredentialsConstants {
	   /** Attribute name: type of context. */
    public static final String TYPE = "type"; // this should be the standard name of type
    public static final String TYPE_UPPER_INITIAL = "Type"; // this is for legacy reasons and it is used, the GUI sends in POST JSON with uppercase initial

    /** Attribute name: server which manages the context. */
    public static final String SERVER = "Server";

    /** Attribute name: Location of certificates and CA signatures. */
    public static final String CERTREPOSITORY = "CertRepository";

    /** Attribute name: Location of an existing certificate proxy to be used. */
    public static final String USERPROXY = "UserProxy";

    /** Attribute name: Location of a user certificate to be used. */
    public static final String USERCERT = "UserCert";

    /** Attribute name: Location of a user key to use. */
    public static final String USERKEY = "UserKey";

    /** Attribute name: User ID or user name to use. */
    public static final String USERID = "UserID";

    /** Attribute name: Password to use. */
    public static final String USERPASS = "UserPass";

    /** Attribute name: The VO the context belongs to. */
    public static final String USERVO = "UserVO";

    /** Attribute name: Time up to which this context is valid. */
    public static final String LIFETIME = "LifeTime";
    
    /** Attribute name: Context types. */
    public static final String TYPE_USERPASS = "UserPass";
    public static final String TYPE_GLOBUS = "Globus";
    public static final String TYPE_VOMS = "VOMS";
    
    public static final String PROJECT = "project";
}
