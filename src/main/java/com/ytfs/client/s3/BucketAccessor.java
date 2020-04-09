package com.ytfs.client.s3;

import com.ytfs.client.v2.YTClient;
import com.ytfs.common.ServiceException;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.s3.GetBucketResp;
import com.ytfs.service.packet.s3.ListBucketResp;
import com.ytfs.service.packet.s3.v2.CreateBucketReqV2;
import com.ytfs.service.packet.s3.v2.DeleteBucketReqV2;
import com.ytfs.service.packet.s3.v2.GetBucketReqV2;
import com.ytfs.service.packet.s3.v2.ListBucketReqV2;
import com.ytfs.service.packet.s3.v2.UpdateBucketReqV2;
import java.util.HashMap;
import java.util.Map;

public class BucketAccessor {

    private final YTClient client;

    public BucketAccessor(YTClient client) {
        this.client = client;
    }

    /**
     * 创建Bucket
     *
     * @param name bucket名称，最大允许100个bucket
     * @param meta bucket属性，自定义数据类型
     * @throws ServiceException
     */
    public void createBucket(String name, byte[] meta) throws ServiceException {
        CreateBucketReqV2 req = new CreateBucketReqV2();
        req.setMeta(meta);
        req.setBucketName(name);
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        P2PUtils.requestBPU(req, client.getSuperNode());
    }

    public void updateBucket(String bucketName, byte[] meta) throws ServiceException {
        UpdateBucketReqV2 req = new UpdateBucketReqV2();
        req.setMeta(meta);
        req.setBucketName(bucketName);
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        P2PUtils.requestBPU(req, client.getSuperNode());
    }

    /**
     * 遍历Bucket名称
     *
     * @return String[]
     * @throws ServiceException
     */
    public String[] listBucket() throws ServiceException {
        ListBucketReqV2 req = new ListBucketReqV2();
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        ListBucketResp resp = (ListBucketResp) P2PUtils.requestBPU(req, client.getSuperNode());
        return resp.getNames();
    }

    public Map<String, byte[]> getBucketByName(String bucketName) throws ServiceException {
        GetBucketReqV2 req = new GetBucketReqV2();
        req.setBucketName(bucketName);
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        GetBucketResp resp = (GetBucketResp) P2PUtils.requestBPU(req, client.getSuperNode());
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
    public void deleteBucket(String bucketName) throws ServiceException {
        DeleteBucketReqV2 req = new DeleteBucketReqV2();
        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
        req.setBucketname(bucketName);
        P2PUtils.requestBPU(req, client.getSuperNode());
    }
}
