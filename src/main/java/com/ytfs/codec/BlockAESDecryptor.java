package com.ytfs.codec;

import static com.ytfs.codec.AESIVParameter.IVParameter;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class BlockAESDecryptor {

    private byte[] srcData;
    private Cipher cipher;
    private final byte[] data;

    public BlockAESDecryptor(byte[] data, byte[] key) {
        this.data = data;
        init(key);
    }

    private void init(byte[] key) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
            cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            IvParameterSpec iv = new IvParameterSpec(IVParameter);
            cipher.init(Cipher.DECRYPT_MODE, skeySpec,iv);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }


    public void decrypt() {
        try {
            this.srcData = cipher.doFinal(data);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    /**
     * @return the srcData
     */
    public byte[] getSrcData() {
        return srcData;
    }
}
