package com.ericsson.oss.eniq.techpack.utils;

import com.ericsson.oss.eniq.techpack.constants.TechPackConstant;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

class AESCrypter {

    void decrypt(InputStream is, OutputStream os, Key key)
            throws NoSuchPaddingException, InvalidKeyException,
            IOException, NoSuchAlgorithmException {
        Cipher cipher = Cipher.getInstance(TechPackConstant.AES_CIPHER_METHOD);
        cipher.init(2, key);
        CipherInputStream cis = new CipherInputStream(is, cipher);
        int in;
        while ((in = cis.read()) != -1)
            os.write(in);
        cis.close();
    }
}