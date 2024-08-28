package com.ericsson.eniq.repository;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;


import java.util.Base64;

final public class AsciiCrypter {
	
	public static Logger log = Logger.getLogger("repository.AsciiCrypter");	
	
	final private static BigInteger BIKEY = new BigInteger ("8842165122180440050");
	final private static String ALGO = "DES";

	private final transient Cipher ecipher;
	private final transient Cipher dcipher;
	private static AsciiCrypter crypter = null;
	
	private AsciiCrypter() throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
		final SecretKey keySpec = new SecretKeySpec(BIKEY.toByteArray(), ALGO);
		ecipher = Cipher.getInstance(ALGO);
		dcipher = Cipher.getInstance(ALGO);
		ecipher.init(Cipher.ENCRYPT_MODE, keySpec);
		dcipher.init(Cipher.DECRYPT_MODE, keySpec);
	}
	
	/**
	 * Gets the instance of this singleton class
	 * @return
	 * @throws Exception 
	 */
	public static AsciiCrypter getInstance () throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException{
		
		synchronized (AsciiCrypter.class) {
			if (crypter == null){
				crypter = new AsciiCrypter();	
			}			
		}
		
		return crypter;
	}
	
	public String encrypt(final String str) throws UnsupportedEncodingException, IllegalBlockSizeException, BadPaddingException {
		
		
		// Encode the string into bytes using utf-8
		final byte[] utf8 = str.getBytes("UTF8");

		// Encrypt
    final byte[] enc;
    synchronized (ecipher) {
      // Not thread safe, can crash the JVM if not synchronized
      enc = ecipher.doFinal(utf8);
    }

		// Encode bytes to base64 to get a string
		//final String retVal =   BASE64Encoder().encode(enc);
    	final String retVal = Base64.getEncoder().encodeToString(enc);
				
		return retVal;
	}

	public String decrypt(final String str) {
		String retVal = null;
		try {
			// Decode base64 to get bytes
			
			//final byte[] dec = new BASE64Decoder().decodeBuffer(str);
			byte[] dec = Base64.getMimeDecoder().decode(str);
			
			// Decrypt
			final byte[] utf8;
      synchronized (dcipher) {
        // Not thread safe, can crash the JVM if not synchronized
        utf8 = dcipher.doFinal(dec);
      }
			
			// Decode using utf-8
			retVal = new String(utf8, "UTF8");
			
			
		} catch (Exception e) {
			
					
				log.log(Level.WARNING,"Exception in AsciiCrypter :" + e.getMessage());
				retVal = str;	

									
		}
		return retVal;
	}

}
