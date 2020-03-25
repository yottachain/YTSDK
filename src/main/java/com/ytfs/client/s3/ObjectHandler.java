package com.ytfs.client.s3;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.s3.*;
import com.ytfs.service.packet.s3.entities.FileMetaMsg;
import org.bson.types.ObjectId;

import java.util.List;

public class ObjectHandler {
    
    

    /**
     * 创建文件元数控
     *
     * @param bucketname
     * @param filename 文件名
     * @param VNU 文件ID
     * @param meta　文件属性，自定义格式
     * @throws ServiceException
     */
    public static void createObject(String bucketname, String filename, ObjectId VNU, byte[] meta) throws ServiceException {
        UploadFileReq req = new UploadFileReq();
        req.setBucketname(bucketname);
        req.setFileName(filename);
        req.setVNU(VNU);
        req.setMeta(meta);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }

//    public static FileMetaMsg getFileMeta(String bucketName,String fileName) throws ServiceException {
//        GetObjectReq req = new GetObjectReq();
//        req.setBucketName(bucketName);
//        req.setFileName(fileName);
//        GetObjectResp resp = (GetObjectResp) P2PUtils.requestBPU(req,UserConfig.superNode);
//        FileMetaMsg fileMeta = new FileMetaMsg();
//        fileMeta.setFileName(resp.getFileName());
//        fileMeta.setVersionId(resp.get);
//    }

    public static FileMetaMsg copyObject(String srcBucket, String srcObjectKey, String destBucket, String destObjectKey) throws ServiceException {
        CopyObjectReq req = new CopyObjectReq();
        req.setSrcBucket(srcBucket);
        req.setSrcObjectKey(srcObjectKey);
        req.setDestBucket(destBucket);
        req.setDestObjectKey(destObjectKey);
        CopyObjectResp resp = (CopyObjectResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        FileMetaMsg fileMeta = new FileMetaMsg();
        fileMeta.setBucketId(resp.getBucketId());
        fileMeta.setFileName(resp.getFileName());
        fileMeta.setMeta(resp.getMeta());
        fileMeta.setVersionId(resp.getVersionId());
        return fileMeta;
    }

    //    public static String listObject(Map<String,byte[]> map, String bucketName, String fileName, int limit) throws ServiceException {
//        ListObjectReq req = new ListObjectReq();
//        req.setBucketName(bucketName);
//        req.setLimit(limit);
//        req.setFileName(fileName);
//        ListObjectResp resp = (ListObjectResp) P2PUtils.requestBPU(req, UserConfig.superNode);
//        map.putAll(resp.getMap());
//        Map<ObjectId,String> lastMap = new HashMap<>();
//        System.out.println("resp.getFileName()====="+resp.getFileName()+"  ,resp.getObjectId()====="+resp.getObjectId());
//        return resp.getFileName();
//    }
    public static List<FileMetaMsg> listBucket(String bucketName, String fileName, String prefix, boolean isVersion, ObjectId nextVersionId, int limit) throws ServiceException{
        ListObjectReq req = new ListObjectReq();
        req.setBucketName(bucketName);
        req.setFileName(fileName);
        req.setLimit(limit);
        req.setPrefix(prefix);
        req.setVersion(isVersion);
        req.setNextVersionId(nextVersionId);
        ListObjectResp resp = (ListObjectResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        List<FileMetaMsg> fileMetaMsgs = resp.getFileMetaMsgList();
        return fileMetaMsgs;
    }

    public static void deleteObject(String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        DeleteFileReq req = new DeleteFileReq();
        req.setBucketname(bucketName);
        req.setFileName(fileName);
        req.setVNU(versionId);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }

    public static boolean isExistObject(String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        boolean isExistObject = false;
        GetObjectReq req = new GetObjectReq();
        req.setBucketName(bucketName);
        req.setFileName(fileName);
        GetObjectResp resp = (GetObjectResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        if (resp.getFileName() != null) {
            isExistObject = true;
        }
        return isExistObject;
    }


    public static ObjectId getObjectIdByName(String bucketName, String fileName) throws ServiceException {
        GetObjectReq req = new GetObjectReq();
        req.setFileName(fileName);
        req.setBucketName(bucketName);
        GetObjectResp resp = (GetObjectResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        ObjectId objectId = resp.getObjectId();
        return objectId;
    }

    public static FileMetaMsg getFileMeta(String bucketName,String fileName) throws ServiceException{
        GetObjectReq req = new GetObjectReq();
        req.setFileName(fileName);
        req.setBucketName(bucketName);
        GetObjectResp resp = (GetObjectResp)P2PUtils.requestBPU(req, UserConfig.superNode);
        FileMetaMsg fileMeta = new FileMetaMsg();
        fileMeta.setMeta(resp.getMeta());
        return fileMeta;
    }
}
