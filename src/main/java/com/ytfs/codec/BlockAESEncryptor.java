package com.ytfs.codec;

import static com.ytfs.codec.AESIVParameter.IVParameter;
import java.io.IOException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class BlockAESEncryptor {

    private final Block block;
    private Cipher cipher;
    private final BlockEncrypted blockEncrypted;

    public BlockAESEncryptor(Block block, byte[] key) {
        this.block = block;
        this.blockEncrypted = new BlockEncrypted();
        this.blockEncrypted.setBlocksize(block.getRealSize());
        init(key);
    }

    private void init(byte[] key) {
        try {//AES/ECB/PKCS5Padding    //ECB/CBC/CTR/CFB/OFB
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
            cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            IvParameterSpec iv = new IvParameterSpec(IVParameter);
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public void encrypt() throws IOException {
        block.load();
        try {
            byte[] bs = cipher.doFinal(block.getData());
            getBlockEncrypted().setData(bs);
        } catch (BadPaddingException | IllegalBlockSizeException r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    /**
     * @return the blockEncrypted
     */
    public BlockEncrypted getBlockEncrypted() {
        return blockEncrypted;
    }
}
