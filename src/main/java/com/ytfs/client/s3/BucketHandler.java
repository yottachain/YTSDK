package com.ytfs.client.s3;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.s3.*;
import java.util.HashMap;
import java.util.Map;

public class BucketHandler {

    /**
     * 创建Bucket
     *
     * @param name bucket名称，最大允许100个bucket
     * @param meta bucket属性，自定义数据类型
     * @throws ServiceException
     */
    public static void createBucket(String name, byte[] meta) throws ServiceException {
        CreateBucketReq req = new CreateBucketReq();
        req.setMeta(meta);
        req.setBucketName(name);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }

    public static void updateBucket(String bucketName, byte[] meta) throws ServiceException {
        UpdateBucketReq req = new UpdateBucketReq();
        req.setMeta(meta);
        req.setBucketName(bucketName);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }

    /**
     * 遍历Bucket名称
     *
     * @return String[]
     * @throws ServiceException
     */
    public static String[] listBucket() throws ServiceException {
        ListBucketReq req = new ListBucketReq();
        ListBucketResp resp = (ListBucketResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        return resp.getNames();
    }

    public static Map<String, byte[]> getBucketByName(String bucketName) throws ServiceException {
        GetBucketReq req = new GetBucketReq();
        req.setBucketName(bucketName);
        GetBucketResp resp = (GetBucketResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        Map<String, byte[]> map = new HashMap<>();
        map.put(bucketName, resp.getMeta());
        return map;
    }

    /**
     * 删除Bucket
     *
     * @param bucketName
     * @throws ServiceException
     */
    public static void deleteBucket(String bucketName) throws ServiceException {
        DeleteBucketReq req = new DeleteBucketReq();
        req.setBucketname(bucketName);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }
}
