package com.ytfs.client.s3;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.s3.*;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.Map;

public class ObjectHandler {

    public static void createObject(String bucketname, String filename, ObjectId VNU,byte[] meta) throws ServiceException {
        UploadFileReq req = new UploadFileReq();
        req.setBucketname(bucketname);
        req.setFileName(filename);
        req.setVNU(VNU);
        req.setMeta(meta);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }
    public static Map<ObjectId,String> listObject(Map<String,byte[]> map,String bucketName,ObjectId startId,int limit) throws ServiceException {
        ListObjectReq req = new ListObjectReq();
        req.setBucketName(bucketName);
        req.setLimit(limit);
        req.setStartId(startId);
        ListObjectResp resp = (ListObjectResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        map.putAll(resp.getMap());
        Map<ObjectId,String> lastMap = new HashMap<>();
        System.out.println("resp.getFileName()====="+resp.getFileName()+"  ,resp.getObjectId()====="+resp.getObjectId());
        if(resp.getFileName()!=null && resp.getObjectId() !=null) {
            lastMap.put(resp.getObjectId(),resp.getFileName());
            return lastMap;
        } else {
            return lastMap;
        }
    }
    public static void deleteObject(String bucketName,String fileName) throws ServiceException {
        DeleteFileReq req = new DeleteFileReq();
        req.setBucketname(bucketName);
        req.setFileName(fileName);
        P2PUtils.requestBPU(req,UserConfig.superNode);
    }
    public static boolean isExistObject(String bucketName,String fileName) throws ServiceException{
        boolean isExistObject = false;
        GetObjectReq req = new GetObjectReq();
        req.setBucketName(bucketName);
        req.setFileName(fileName);
        GetObjectResp resp = (GetObjectResp)P2PUtils.requestBPU(req, UserConfig.superNode);
        if(resp.getFileName() != null) {
            isExistObject = true;
        }
        return isExistObject;
    }
    public static ObjectId getObjectIdByName(String bucketName,String fileName) throws ServiceException{
        GetObjectReq req = new GetObjectReq();
        req.setFileName(fileName);
        req.setBucketName(bucketName);
        GetObjectResp resp = (GetObjectResp)P2PUtils.requestBPU(req, UserConfig.superNode);
        ObjectId objectId = resp.getObjectId();
        return objectId;
    }
}
