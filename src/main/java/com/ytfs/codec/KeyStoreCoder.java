package com.ytfs.codec;

import static io.jafka.jeos.util.KeyUtil.secp;
import io.jafka.jeos.util.Raw;
import io.jafka.jeos.util.ecc.Hex;
import io.jafka.jeos.util.ecc.Point;
import java.math.BigInteger;
import java.security.Key;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.ECPoint;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NullCipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import sun.security.ec.ECPrivateKeyImpl;
import sun.security.ec.ECPublicKeyImpl;
import sun.security.util.ECUtil;

public class KeyStoreCoder {

    /**
     * 生成随机密钥
     *
     * @return
     */
    public static byte[] generateRandomKey() {
        long r = Math.round(Math.random() * Long.MAX_VALUE);
        long l = System.currentTimeMillis();
        byte[] bs = new byte[]{(byte) (r >>> 56), (byte) (r >>> 48), (byte) (r >>> 40), (byte) (r >>> 32),
            (byte) (r >>> 24), (byte) (r >>> 16), (byte) (r >>> 8), (byte) (r),
            (byte) (l >>> 56), (byte) (l >>> 48), (byte) (l >>> 40), (byte) (l >>> 32),
            (byte) (l >>> 24), (byte) (l >>> 16), (byte) (l >>> 8), (byte) (l)
        };
        try {
            KeyGenerator kgen = KeyGenerator.getInstance("AES");
            kgen.init(256, new SecureRandom(bs));
            SecretKey secretKey = kgen.generateKey();
            byte[] enCodeFormat = secretKey.getEncoded();
            return enCodeFormat;
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    /**
     * 加密随即密钥
     *
     * @param data
     * @param kd 去重用密钥KD
     * @return ked 32字节
     */
    public static byte[] aesEncryped(byte[] data, byte[] kd) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(kd, "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            byte[] bs = cipher.doFinal(data);
            byte[] res = new byte[32];
            System.arraycopy(bs, 0, res, 0, 32);
            return res;
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    /**
     * 解密随即密钥
     *
     * @param data
     * @param kd 去重用密钥KD
     * @return ks 16字节
     */
    public static byte[] aesDecryped(byte[] data, byte[] kd) {
        try {
            SecretKeySpec skeySpec1 = new SecretKeySpec(kd, "AES");
            Cipher cipher1 = Cipher.getInstance("AES");
            cipher1.init(Cipher.ENCRYPT_MODE, skeySpec1);
            byte[] end = cipher1.doFinal();
            byte[] srcdata = new byte[data.length + 16];
            System.arraycopy(data, 0, srcdata, 0, data.length);
            System.arraycopy(end, 0, srcdata, data.length, 16);
            SecretKeySpec skeySpec = new SecretKeySpec(kd, "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            return cipher.doFinal(srcdata);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    private static final String SIGNATURE_ALGORITHM = "SHA512withECDSA";
    private static final String KEY_ALGORITHM = "secp256k1";

    public static Key generatePrivateKey(byte[] private_wif) {
        byte version = (byte) 0x80;
        if (private_wif[0] != version) {
            throw new IllegalArgumentException("Expected version " + 0x80 + ", instead got " + version);
        }
        try {
            byte[] private_key = Raw.copy(private_wif, 0, private_wif.length - 4);
            byte[] last_private_key = Raw.copy(private_key, 1, private_key.length - 1);
            BigInteger iKey = new BigInteger(Hex.toHex(last_private_key), 16);
            ECPrivateKeyImpl priKey = new ECPrivateKeyImpl(iKey, ECUtil.getECParameterSpec((Provider) null, KEY_ALGORITHM));
            return priKey;
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public static Key generatePublicKey(byte[] pubkey) {
        try {
            byte[] pub_buf = Raw.copy(pubkey, 0, pubkey.length - 4);
            Point ep = secp.getCurve().decodePoint(pub_buf);
            ECPoint ecpoint = new ECPoint(ep.getX().toBigInteger(), ep.getY().toBigInteger());
            ECPublicKeyImpl puk = new ECPublicKeyImpl(ecpoint, ECUtil.getECParameterSpec((Provider) null, KEY_ALGORITHM));
            return puk;
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public static byte[] eccEncryped(byte[] data, byte[] pubkey) {
        try {
            ECPublicKeyImpl pkey = (ECPublicKeyImpl) generatePublicKey(pubkey);
            Cipher cipher = new NullCipher();
            cipher.init(Cipher.ENCRYPT_MODE, pkey, pkey.getParams());
            byte[] bs = cipher.doFinal(data);
            return bs;
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public static byte[] eccDecryped(byte[] data, byte[] prikey) {
        try {
            ECPrivateKeyImpl privateK = (ECPrivateKeyImpl) generatePrivateKey(prikey);
            Cipher cipher = new NullCipher();
            cipher.init(Cipher.DECRYPT_MODE, privateK, privateK.getParams());
            return cipher.doFinal(data);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public static byte[] ecdsaSign(byte[] data, byte[] prikey) {
        try {
            ECPrivateKeyImpl privateK = (ECPrivateKeyImpl) generatePrivateKey(prikey);
            Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
            signature.initSign(privateK);
            signature.update(data);
            return signature.sign();
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public static boolean ecdsaVerify(byte[] data, byte[] sign, byte[] pubkey) {
        try {
            ECPublicKeyImpl publicKey = (ECPublicKeyImpl) generatePublicKey(pubkey);
            Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
            signature.initVerify(publicKey);
            signature.update(data);
            return signature.verify(sign);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

}
