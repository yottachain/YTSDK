package com.ytfs.client.s3;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.s3.ListBucketReq;
import com.ytfs.service.packet.s3.ListBucketResp;
import com.ytfs.service.packet.s3.UploadFileReq;
import org.bson.types.ObjectId;

public class ObjectHandler {

    public static void createObject(String bucketname, String filename, ObjectId VNU,byte[] meta) throws ServiceException {
        UploadFileReq req = new UploadFileReq();
        req.setBucketname(bucketname);
        req.setFileName(filename);
        req.setVNU(VNU);
        req.setMeta(meta);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }
    public static String[] listObject(String bucketName) throws ServiceException {
        ListBucketReq req = new ListBucketReq();
        ListBucketResp resp = (ListBucketResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        return resp.getNames();
    }
}
