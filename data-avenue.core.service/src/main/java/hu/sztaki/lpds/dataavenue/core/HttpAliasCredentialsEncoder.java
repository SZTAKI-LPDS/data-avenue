package hu.sztaki.lpds.dataavenue.core;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import sun.misc.BASE64Decoder;
//import sun.misc.BASE64Encoder;
import org.apache.commons.codec.binary.Base64;

/*
 * This class encrypts sensitive alias credentials data before stored in database, and aso decrypts them when fetched from database.
 * It used AES (Java JCE), as it is considered fast and secure enough. 
 */
public class HttpAliasCredentialsEncoder {
	private static final Logger log = LoggerFactory.getLogger(HttpAliasCredentialsEncoder.class);

	private static final String ALGORITHM = "AES";
	private static final String CHECK_PREFIX = "OK";
	private static final String KEY_FILLER = "                "; // exaclty 16 chars long
	private static final int KEY_LENGTH = KEY_FILLER.length();

	private Cipher encoder = null, decoder = null;
	
	private static final HttpAliasCredentialsEncoder INSTANCE = new HttpAliasCredentialsEncoder(); // singleton
	public static HttpAliasCredentialsEncoder getInstance() { return INSTANCE; }
	
	HttpAliasCredentialsEncoder() {
		initCipher(Configuration.getAliasCredentialsEncriptionKey());
	}
	
	public void initCipher(final String key) {
		log.debug("Initializing HTTP alias encoder/decoder with secret key...");
		if (key == null) throw new IllegalArgumentException("Invalid encryption key! (null)");
		String secretKey = key.length() > KEY_LENGTH ? secretKey = key.substring(0, KEY_LENGTH) : key + KEY_FILLER.substring(0, KEY_LENGTH - key.length());
		synchronized (this) {
			try { 
				encoder = Cipher.getInstance(ALGORITHM); 
				decoder = Cipher.getInstance(ALGORITHM);
			} 
			catch (NoSuchAlgorithmException e) { log.error("Cannot get cypher: " + ALGORITHM + "!", e); return; }
			catch (NoSuchPaddingException e) { log.error("Cannot get cypher: " + ALGORITHM + "!", e); return; }
			
			SecretKeySpec encryptionKey = new SecretKeySpec(secretKey.getBytes(), ALGORITHM);
			
			try {
				encoder.init(Cipher.ENCRYPT_MODE, encryptionKey);
				decoder.init(Cipher.DECRYPT_MODE, encryptionKey, decoder.getParameters());
			} catch (InvalidKeyException e) {
				log.error("Cannot set cypher secret key!", e);
				encoder = decoder = null;
				return; 
			}
			catch (InvalidAlgorithmParameterException e) {
				log.error("Cannot set cypher secret key!", e);
				encoder = decoder = null;
			}
		}
	}
	
	String encrypt(final String stringToEncrypt) {
		if (encoder == null) return stringToEncrypt;
		synchronized (this) {
			try {
				byte [] byteCipherText = encoder.doFinal((CHECK_PREFIX + stringToEncrypt).getBytes());
				return new String(Base64.encodeBase64(byteCipherText));
			} 
			catch (IllegalBlockSizeException e) { log.error("Cannot encrypt data!", e); return stringToEncrypt; } 
			catch (BadPaddingException e) { log.error("Cannot encrypt data!", e); return stringToEncrypt; }
		}
	}
	
	String decrypt(final String base64StringToDecript) throws Exception {
		if (encoder == null) return base64StringToDecript;
		synchronized (this) {
			try { 
				byte [] bytesToDecrypt = Base64.decodeBase64(base64StringToDecript.getBytes());
				byte [] bytesDecrypted = decoder.doFinal(bytesToDecrypt);
				String stringDecriptor = new String(bytesDecrypted);
				if (!stringDecriptor.startsWith(CHECK_PREFIX)) throw new Exception("Inconsistent decrypted data"); // secret key changed?
				return stringDecriptor.substring(CHECK_PREFIX.length(), bytesDecrypted.length); 
			} 
			catch (IllegalBlockSizeException e) { log.error("Cannot decrypt data!", e); return base64StringToDecript; } 
			catch (BadPaddingException e) { log.error("Cannot decrypt data!", e); return base64StringToDecript; } 
			catch (IOException e) { log.error("Cannot decrypt data!", e); return base64StringToDecript; }
		}
	}
}