package com.ytfs.client;

import com.ytfs.service.UserConfig;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.packet.CreateBucketReq;
import com.ytfs.service.packet.ListBucketReq;
import com.ytfs.service.packet.ListBucketResp;
import com.ytfs.service.packet.ServiceException;

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
