package com.ytfs.codec;

import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;

public class EccTest {

    public static void main(String[] args) throws Exception {

        String prikey = "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx";
        byte[] kusp = Base58.decode(prikey);//si钥    
        String pubkey = KeyUtil.toPublicKey(prikey);
        pubkey = pubkey.substring(3);
        System.out.println(prikey);
        System.out.println(pubkey);
        byte[] kuep = Base58.decode(pubkey);//gong钥    

        byte[] data = "sdsfagergwhge6h56".getBytes();
        byte[] bs = KeyStoreCoder.ecdsaSign(data, kusp);
        boolean c = KeyStoreCoder.ecdsaVerify(data, bs, kuep);
        System.out.println(c);
    }

    public static void crype() throws Exception {
        byte[] ed = KeyStoreCoder.generateRandomKey();

        Block b = new Block("sdsfagergwhge6h56".getBytes());
        b.calculate();

        byte[] bss = KeyStoreCoder.aesEncryped(ed, b.getKD());
        byte[] bss1 = KeyStoreCoder.aesDecryped(bss, b.getKD());

        String prikey = "5KQKydL7TuRwjzaFSK4ezH9RUXWuYHW1yYDp5CmQfsfTuu9MBLZ";
        byte[] kusp = Base58.decode(prikey);//si钥    
        String pubkey = KeyUtil.toPublicKey(prikey);
        pubkey = pubkey.substring(3);
        System.out.println(prikey);
        System.out.println(pubkey);
        byte[] kuep = Base58.decode(pubkey);//gong钥    

        String sss = "dsfaaaaaafgdhytjtrjrytuj";
        System.out.println(sss);
        byte[] data = KeyStoreCoder.eccEncryped(sss.getBytes(), kuep);
        byte[] bs = KeyStoreCoder.eccDecryped(data, kusp);
        System.out.println(new String(bs));
    }

}
