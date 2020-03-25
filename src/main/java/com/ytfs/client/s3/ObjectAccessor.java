package com.ytfs.client.s3;

import com.ytfs.client.v2.YTClient;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.s3.CopyObjectResp;
import com.ytfs.service.packet.s3.GetObjectReq;
import com.ytfs.service.packet.s3.GetObjectResp;
import com.ytfs.service.packet.s3.ListObjectResp;
import com.ytfs.service.packet.s3.entities.FileMetaMsg;
import com.ytfs.service.packet.s3.v2.CopyObjectReqV2;
import com.ytfs.service.packet.s3.v2.DeleteFileReqV2;
import com.ytfs.service.packet.s3.v2.GetObjectReqV2;
import com.ytfs.service.packet.s3.v2.ListObjectReqV2;
import com.ytfs.service.packet.s3.v2.UploadFileReqV2;
import java.util.List;
import org.bson.types.ObjectId;

public class ObjectAccessor {

    private final YTClient client;

    public ObjectAccessor(YTClient client) {
        this.client = client;
    }

    /**
     * 创建文件元数控
     *
     * @param bucketname
     * @param filename 文件名
     * @param VNU 文件ID
     * @param meta　文件属性，自定义格式
     * @throws ServiceException
     */
    public void createObject(String bucketname, String filename, ObjectId VNU, byte[] meta) throws ServiceException {
        UploadFileReqV2 req = new UploadFileReqV2();
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        req.setBucketname(bucketname);
        req.setFileName(filename);
        req.setVNU(VNU);
        req.setMeta(meta);
        P2PUtils.requestBPU(req, client.getSuperNode());
    }

    public FileMetaMsg copyObject(String srcBucket, String srcObjectKey, String destBucket, String destObjectKey) throws ServiceException {
        CopyObjectReqV2 req = new CopyObjectReqV2();
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        req.setSrcBucket(srcBucket);
        req.setSrcObjectKey(srcObjectKey);
        req.setDestBucket(destBucket);
        req.setDestObjectKey(destObjectKey);
        CopyObjectResp resp = (CopyObjectResp) P2PUtils.requestBPU(req, client.getSuperNode());
        FileMetaMsg fileMeta = new FileMetaMsg();
        fileMeta.setBucketId(resp.getBucketId());
        fileMeta.setFileName(resp.getFileName());
        fileMeta.setMeta(resp.getMeta());
        fileMeta.setVersionId(resp.getVersionId());
        return fileMeta;
    }

    public List<FileMetaMsg> listBucket(String bucketName, String fileName, String prefix, boolean isVersion, ObjectId nextVersionId, int limit) throws ServiceException {
        ListObjectReqV2 req = new ListObjectReqV2();
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        req.setBucketName(bucketName);
        req.setFileName(fileName);
        req.setLimit(limit);
        req.setPrefix(prefix);
        req.setVersion(isVersion);
        req.setNextVersionId(nextVersionId);
        ListObjectResp resp = (ListObjectResp) P2PUtils.requestBPU(req, client.getSuperNode());
        List<FileMetaMsg> fileMetaMsgs = resp.getFileMetaMsgList();
        return fileMetaMsgs;
    }

    public void deleteObject(String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        DeleteFileReqV2 req = new DeleteFileReqV2();
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        req.setBucketname(bucketName);
        req.setFileName(fileName);
        req.setVNU(versionId);
        P2PUtils.requestBPU(req, client.getSuperNode());
    }

    public boolean isExistObject(String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        boolean isExistObject = false;
        GetObjectReqV2 req = new GetObjectReqV2();
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        req.setBucketName(bucketName);
        req.setFileName(fileName);
        GetObjectResp resp = (GetObjectResp) P2PUtils.requestBPU(req, client.getSuperNode());
        if (resp.getFileName() != null) {
            isExistObject = true;
        }
        return isExistObject;
    }

    public ObjectId getObjectIdByName(String bucketName, String fileName) throws ServiceException {
        GetObjectReqV2 req = new GetObjectReqV2();
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        req.setFileName(fileName);
        req.setBucketName(bucketName);
        GetObjectResp resp = (GetObjectResp) P2PUtils.requestBPU(req, client.getSuperNode());
        ObjectId objectId = resp.getObjectId();
        return objectId;
    }
}
