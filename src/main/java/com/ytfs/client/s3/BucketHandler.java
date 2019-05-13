package com.ytfs.client.s3;

import com.ytfs.client.UserConfig;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.packet.s3.CreateBucketReq;
import com.ytfs.service.packet.s3.ListBucketReq;
import com.ytfs.service.packet.s3.ListBucketResp;
import com.ytfs.service.ServiceException;

public class BucketHandler {

    public static void createBucket(String name) throws ServiceException {
        CreateBucketReq req = new CreateBucketReq();
        req.setBucketName(name);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }

    public static String[] listBucket() throws ServiceException {
        ListBucketReq req = new ListBucketReq();
        ListBucketResp resp = (ListBucketResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        return resp.getNames();
    }
}
