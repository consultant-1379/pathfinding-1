package com.ericsson.oss.eniq.techpack.utils;

import com.ericsson.oss.eniq.techpack.constants.TechPackConstant;
import com.ericsson.oss.eniq.techpack.globalexception.TechPackExceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.util.Enumeration;
import java.util.zip.*;

@Component
public class ZipCrypter {

    private static final Logger logger = LoggerFactory.getLogger(ZipCrypter.class);

    private static final BigInteger DEFAULT_KEY_MOD = new BigInteger(
            "123355219375882378192369770441285939191353866566017497282747046709534536708757928527167390021388683110840288891057176815668475724440731714035455547579744783774075008195670576737607241438665521837871490309744873315551646300131908174140715653425601662203921855253249615512397376967139410627761058910648132466577");
    private static final BigInteger DEFAULT_KEY_EXP = BigInteger.valueOf(65537);

    private int cryptMode;
    private BigInteger keyMod;
    private BigInteger keyExp;
    private boolean isPublic;
    private Key rsaKey;

    public class ZipCrypterDataEntry {
        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }

        private byte[] getExtra() {
            return extra;
        }

        private int getSize() {
            return data.length;
        }

        private byte[] data;
        private byte[] extra;
        final ZipCrypter crypter;

        private ZipCrypterDataEntry() {
            super();
            crypter = ZipCrypter.this;
        }
    }

    public ZipCrypter() {
        cryptMode = 2;
        keyMod = null;
        keyExp = null;
        isPublic = true;
        rsaKey = null;
    }

    public void executeDecryptFile(File fileTarget) throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        initPublicKey();

        if (fileTarget != null) {
            File[] fileList = fileTarget.listFiles();
            if (fileList != null) {
                for (File file : fileList) {
                    callTPIDecrypt(file);
                }
                File[] files = tpiFileUnzip(fileTarget);
                deleteZipFile(files);
            }
        } else {
            throw new TechPackExceptions("The target file has not been specified.");
        }
    }

    private void callTPIDecrypt(File file) throws IOException {
        if (file.getName().endsWith(".tpi")) {
            decryptZipFile(file);
            if (file.isFile() || file.getName().endsWith(".tpi")) {
                Path dir = Paths.get(file.getAbsolutePath());
                Files.delete(dir);
            }
        }
    }

    private void deleteZipFile(File[] zipList) throws IOException {
        for (File zipFile : zipList) {
            if (zipFile.getName().endsWith(".zip")) {
                Path dir = Paths.get(zipFile.getAbsolutePath());
                Files.delete(dir);
            }
        }
    }

    private File[] tpiFileUnzip(File fileTarget) {
        File[] fileList = fileTarget.listFiles();
        if (fileList != null) {
            for (File zipFile : fileList) {
                if (zipFile.isFile() || zipFile.getName().endsWith(".zip")) {
                    unzipTPIFile(zipFile);
                }
            }
        }
        return fileList;
    }

    private void decryptZipFile(File outputFile) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(bos);
        zos.setMethod(8);

        try (ZipFile zf = new ZipFile(outputFile)) {
            Enumeration<?> entries = zf.entries();
            while (entries.hasMoreElements()) {
                ZipEntry ze = (ZipEntry) entries.nextElement();
                if (!ze.isDirectory()) {
                    ZipEntry newEntry = new ZipEntry(ze.getName());
                    ZipCrypterDataEntry output = cryptInputStream(
                            zf.getInputStream(ze), cryptMode, ze.getExtra(),
                            rsaKey);
                    newEntry.setSize(output.getSize());
                    newEntry.setExtra(output.getExtra());
                    newEntry.setTime(ze.getTime());
                    CRC32 crc = new CRC32();
                    crc.update(output.getData());
                    newEntry.setCrc(crc.getValue());

                    zos.putNextEntry(newEntry);
                    zos.write(output.getData());
                    zos.closeEntry();
                }
            }
            zos.flush();
            zos.close();
            String newZipFile = outputFile.getAbsolutePath().replaceAll(".tpi", ".zip");
            logger.info("newZipFile : {} ", newZipFile);
            createFile(bos, new File(newZipFile));
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    private void unzipTPIFile(File outputFile) {
        String fileFolderName = outputFile.getName().replace(".zip", "");
        String parentFileName = outputFile.getParent();
        String destDirectory = parentFileName + File.separator + fileFolderName;
        logger.info("Destination directory path : {}", destDirectory);
        FileInputStream fis;
        byte[] buffer = new byte[1024];
        try {
            fis = new FileInputStream(outputFile);
            try (ZipInputStream zis = new ZipInputStream(fis)) {
                ZipEntry ze = zis.getNextEntry();
                while (ze != null) {
                    String fileName = ze.getName();
                    File newFile = new File(destDirectory + File.separator + fileName);
                    new File(newFile.getParent()).mkdirs();

                    try (FileOutputStream fos = new FileOutputStream(newFile)) {
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                        fos.flush();
                        zis.closeEntry();
                        ze = zis.getNextEntry();

                    }
                }
                zis.closeEntry();
                fis.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void createFile(ByteArrayOutputStream bos, File fileTarget) {
        try (FileOutputStream outStream = new FileOutputStream(fileTarget)) {
            bos.flush();
            outStream.write(bos.toByteArray());
            outStream.flush();
            bos.close();
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    private ZipCrypterDataEntry cryptInputStream(InputStream is, int mode,
                                                 byte[] extra, Key key) {
        ZipCrypterDataEntry returnEntry = new ZipCrypterDataEntry();
        AESCrypter aes = new AESCrypter();
        ByteArrayOutputStream entryBos = new ByteArrayOutputStream();
        Key aesKey = null;
        if (mode == 2) {
            try {
                aesKey = decryptAESKey(extra, key);
                int in;
                if (aesKey == null) {
                    while ((in = is.read()) != -1)
                        entryBos.write(in);
                } else {
                    aes.decrypt(is, entryBos, aesKey);
                }
            } catch (Exception e) {
                logger.info(e.getMessage());
            }
        }
        returnEntry.setData(entryBos.toByteArray());
        return returnEntry;
    }

    private Key decryptAESKey(byte[] encrypted, Key rsaKey2) throws NoSuchAlgorithmException,
            NoSuchPaddingException, InvalidKeyException {
        if (encrypted == null || encrypted.length == 0) {
            return null;
        }
        Cipher cipher = Cipher.getInstance(TechPackConstant.DEFAULT_CIPHER);
        cipher.init(2, rsaKey2);
        byte[] key;
        try {
            key = cipher.doFinal(encrypted);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return new SecretKeySpec(key, TechPackConstant.AES_NAME);
    }

    private void initPublicKey()
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        if (keyMod == null || keyExp == null) {
            keyMod = DEFAULT_KEY_MOD;
            keyExp = DEFAULT_KEY_EXP;
        }
        if (isPublic) {
            rsaKey = getPublicKey(keyMod, keyExp);
        }
    }

    private static PublicKey getPublicKey(BigInteger publicMode, BigInteger pubExp)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        RSAPublicKeySpec keySpec = new RSAPublicKeySpec(publicMode, pubExp);
        KeyFactory keyFactory = KeyFactory.getInstance(TechPackConstant.DEFAULT_CIPHER_NAME);
        return keyFactory.generatePublic(keySpec);
    }

    public void execute(File fileTarget) throws InvalidKeySpecException, NoSuchAlgorithmException {

        initPublicKey();
        if (fileTarget == null)
            throw new TechPackExceptions("The target file has not been specified.");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(bos);
        zos.setMethod(8);

        try (ZipFile zf = new ZipFile(fileTarget)) {
            Enumeration<?> entries = zf.entries();
            do {
                if (!entries.hasMoreElements())
                    break;
                ZipEntry ze = (ZipEntry) entries.nextElement();
                if (!ze.isDirectory()) {
                    ZipEntry newEntry = new ZipEntry(ze.getName());
                    ZipCrypterDataEntry output = cryptInputStream(
                            zf.getInputStream(ze), cryptMode, ze.getExtra(),
                            rsaKey);
                    newEntry.setSize(output.getSize());
                    newEntry.setExtra(output.getExtra());
                    newEntry.setTime(ze.getTime());
                    CRC32 crc = new CRC32();
                    crc.update(output.getData());
                    newEntry.setCrc(crc.getValue());

                    zos.putNextEntry(newEntry);
                    zos.write(output.getData());
                    zos.closeEntry();
                }
            } while (true);
            zos.flush();
            zos.close();

            logger.info("FeatureZip file decrypted successfully : {}", fileTarget);
            decryptFeatureZipFile(bos, fileTarget);
            featureFileUnzip(fileTarget);
            logger.info("FeatureZip file unzip successfully : {}", fileTarget);
        } catch (Exception e) {
            throw new TechPackExceptions("");
        }
    }

    private void decryptFeatureZipFile(ByteArrayOutputStream bos, File fileTarget) throws IOException {
        if (!fileTarget.exists() || fileTarget.canWrite()) {
            try (OutputStream outStream = new FileOutputStream(fileTarget)) {
                bos.flush();
                outStream.write(bos.toByteArray());
                outStream.flush();
                bos.close();
            } catch (FileNotFoundException e) {
                throw new TechPackExceptions("Could not find the file.");
            }
        }
    }

    private void featureFileUnzip(File outputFile) {

        String fileFolderName = outputFile.getAbsolutePath().replace(".zip", "");
        logger.info("fileFolderName : {}", fileFolderName);

        File dir = new File(fileFolderName);
        byte[] buffer = new byte[1024];
        try (FileInputStream fis = new FileInputStream(outputFile);
             ZipInputStream zis = new ZipInputStream(fis)) {


            ZipEntry ze = zis.getNextEntry();
            while (ze != null) {
                String fileName = ze.getName();
                File newFile = new File(dir + File.separator + fileName);
                new File(newFile.getParent()).mkdirs();
                try (FileOutputStream fos = new FileOutputStream(newFile)) {
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }
                    fos.flush();
                    zis.closeEntry();
                    ze = zis.getNextEntry();
                }

            }
            zis.closeEntry();
        } catch (Exception e) {
            logger.info(" Exception Occured  in ZypCrypter {}", e.getMessage());
        }
    }

}
