package com.ytfs.client.s3;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.s3.*;
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
        ListObjectReq req = new ListObjectReq();
        ListObjectResp resp = (ListObjectResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        return resp.getNames();
    }
}
