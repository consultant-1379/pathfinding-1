package com.ericsson.eniq.repository;

import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.junit.Test;

public class AsciiCryperTest {

	@Test
	public void testCrypting() throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, UnsupportedEncodingException, IllegalBlockSizeException, BadPaddingException {
		final AsciiCrypter crypter = AsciiCrypter.getInstance();
		final String input = "etlrep";
		final String output = crypter.encrypt(input);
		
		
		System.out.println("o/p of encrypting: "+output);
		if (input.equals(output)) {
			fail("Crypting did not work");
		}
		
		final String backToAscii = crypter.decrypt(output);
		if (!backToAscii.equals(input)){
			fail("Decrypting did not work");			
		}
	}

	@Test
	public void testClassConstruction() throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException {
		final AsciiCrypter cryper1 = AsciiCrypter.getInstance();
		final AsciiCrypter cryper2 = AsciiCrypter.getInstance();

		if (cryper1 != cryper2) {
			fail("Singleton method did not return same object");
		}
	}

}
