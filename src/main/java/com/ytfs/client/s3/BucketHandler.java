package com.ytfs.client.s3;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.s3.*;

public class BucketHandler {

    public static void createBucket(String name,byte[] meta) throws ServiceException {
        CreateBucketReq req = new CreateBucketReq();
        req.setMeta(meta);
        req.setBucketName(name);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }

    public static String[] listBucket() throws ServiceException {
        ListBucketReq req = new ListBucketReq();
        ListBucketResp resp = (ListBucketResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        return resp.getNames();
    }

    public static GetBucketResp getBucketByName(String bucketName) throws ServiceException {

        GetBucketReq req = new GetBucketReq();
        req.setBucketName(bucketName);
        GetBucketResp resp = (GetBucketResp) P2PUtils.requestBP(req,UserConfig.superNode);
        return resp;
    }

    public static void deleteBucket(String bucketName) throws ServiceException {
        DeleteBucketReq req = new DeleteBucketReq();
        req.setBucketname(bucketName);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }
}
