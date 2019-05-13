package com.ytfs.codec;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class AESIVParameter {

    public static final byte[] IVParameter = init();

    private static byte[] init() {
        byte[] res = null;
        try {
            String parameter = "YottaChain2018王东临侯月文韩大光";
            byte[] bs = parameter.getBytes(Charset.forName("utf-8"));
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            res = md5.digest(bs);
        } catch (NoSuchAlgorithmException ex) {
        }
        return res;
    }
}
