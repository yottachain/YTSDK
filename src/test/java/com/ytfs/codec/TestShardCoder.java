package com.ytfs.codec;

import java.security.MessageDigest;
import java.util.List;
import org.apache.commons.codec.binary.Hex;

public class TestShardCoder {

    private static byte[] key = KeyStoreCoder.generateRandomKey();

    public static void main(String[] args) throws Exception {
        middleBlock();
        //smallBlock();
    }

    private static void middleBlock() throws Exception {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        key = sha256.digest(key);

        Block block = new Block("d:\\YottaChain架构文档.docx");
        block.load();

        BlockAESEncryptor aes = new BlockAESEncryptor(block, key);
        aes.encrypt();
        int encryptedBlockSize = aes.getBlockEncrypted().getEncryptedBlockSize();

        ShardRSEncoder encoder = new ShardRSEncoder(aes.getBlockEncrypted());
        encoder.encode();

        List<Shard> shards = encoder.getShardList();

        deleteDataShard(shards);
        //deleteParityShard(shards);

        ShardRSDecoder decoder = new ShardRSDecoder(shards, encryptedBlockSize);
        BlockEncrypted b = decoder.decode();

        BlockAESDecryptor aesdecoder = new BlockAESDecryptor(b.getData(),   key);
        aesdecoder.decrypt();

        block = new Block(aesdecoder.getSrcData());
        block.save("d:\\YottaChain.docx");

    }

    private static void smallBlock() throws Exception {
        Block block = new Block("d:\\aa.txt");
        block.load();
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        key = sha256.digest("123456".getBytes());
        // System.out.println(Hex.encodeHexString(block.getData()));
        BlockAESEncryptor aes = new BlockAESEncryptor(block, key);
        aes.encrypt();

        BlockEncrypted b = aes.getBlockEncrypted();

        System.out.println(Hex.encodeHexString(b.getData()));
        //int encryptedBlockSize = aes.getBlockEncrypted().getEncryptedBlockSize();

        //ShardRSEncoder encoder = new ShardRSEncoder(aes.getBlockEncrypted());
        //encoder.encode();
        // List<Shard> shards = encoder.getShardList();
        //deleteDataShard(shards);
        //deleteParityShard(shards);
        // ShardRSDecoder decoder = new ShardRSDecoder(shards, encryptedBlockSize);
        // BlockEncrypted b = decoder.decode();
        BlockAESDecryptor aesdecoder = new BlockAESDecryptor(b.getData(), key);
        aesdecoder.decrypt();
        block = new Block(aesdecoder.getSrcData());
        block.save("d:\\cc.txt");
    }

    private static void deleteDataShard(List<Shard> shards) {
        shards.remove(2);
        shards.remove(5);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        /*
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);

        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
         */
        //shards.remove(7);
    }

    private static void deleteParityShard(List<Shard> shards) {
        shards.remove(shards.size() - 2);
        shards.remove(shards.size() - 2);
        shards.remove(shards.size() - 2);
    }
}
