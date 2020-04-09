package com.ytfs.client.v2;

import com.ytfs.client.DownloadObject;
import com.ytfs.client.UploadObject;
import com.ytfs.client.s3.BucketAccessor;
import com.ytfs.client.s3.ObjectAccessor;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.KeyStoreCoder;
import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import org.bson.types.ObjectId;

public class YTClient {

    private final String username;
    private final String privateKey; //用户私钥
    private final byte[] KUSp;
    private final byte[] AESKey;
    private int userId;
    private int keyNumber;
    private SuperNode superNode;
    private final String accessorKey; //用户私钥

    public ObjectAccessor createObjectAccessor() {
        return new ObjectAccessor(this);
    }

    public BucketAccessor createBucketAccessor() {
        return new BucketAccessor(this);
    }

    public DownloadObject createDownloadObject(byte[] VHW) throws ServiceException {
        return new DownloadObject(this, VHW);
    }

    public DownloadObject createDownloadObject(String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        return new DownloadObject(this, bucketName, fileName, versionId);
    }

    public UploadObject createUploadObject(String path) throws IOException {
        return new UploadObject(this, path);
    }

    public UploadObject createUploadObject(byte[] data) throws IOException {
        return new UploadObject(this, data);
    }

    public YTClient(String username, String privateKey) throws IOException {
        this.username = username;
        this.privateKey = privateKey;
        this.KUSp = Base58.decode(privateKey);
        if (KUSp.length != 37) {
            throw new IOException();
        }
        this.AESKey = KeyStoreCoder.generateUserKey(KUSp);
        String pubkey = KeyUtil.toPublicKey(privateKey);
        pubkey = pubkey.substring(3);
        this.accessorKey = pubkey;
    }

    /**
     * @param userId the userId to set
     */
    public void setUserId(int userId) {
        this.userId = userId;
    }

    /**
     * @param keyNumber the keyNumber to set
     */
    public void setKeyNumber(int keyNumber) {
        this.keyNumber = keyNumber;
    }

    /**
     * @param superNode the superNode to set
     */
    public void setSuperNode(SuperNode superNode) {
        this.superNode = superNode;
    }

    /**
     * @return the userId
     */
    public int getUserId() {
        return userId;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the privateKey
     */
    public String getPrivateKey() {
        return privateKey;
    }

    /**
     * @return the KUSp
     */
    public byte[] getKUSp() {
        return KUSp;
    }

    /**
     * @return the keyNumber
     */
    public int getKeyNumber() {
        return keyNumber;
    }

    /**
     * @return the AESKey
     */
    public byte[] getAESKey() {
        return AESKey;
    }

    /**
     * @return the superNode
     */
    public SuperNode getSuperNode() {
        return superNode;
    }

    /**
     * @return the publicKey
     */
    public String getAccessorKey() {
        return accessorKey;
    }

}
