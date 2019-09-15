package com.ytfs.client;

import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.codec.Shard;
import com.ytfs.common.conf.UserConfig;
import static com.ytfs.common.conf.UserConfig.UPLOADSHARDTHREAD;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.UploadShard2CResp;
import com.ytfs.service.packet.UploadShardReq;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.service.packet.node.GetNodeCapacityReq;
import com.ytfs.service.packet.node.GetNodeCapacityResp;
import io.jafka.jeos.util.Base58;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;

public class UploadShard implements Runnable {

    private static final Logger LOG = Logger.getLogger(UploadShard.class);
    private static ArrayBlockingQueue<UploadShard> queue = null;

    private static synchronized ArrayBlockingQueue<UploadShard> getQueue() {
        if (queue == null) {
            queue = new ArrayBlockingQueue(UPLOADSHARDTHREAD);
            for (int ii = 0; ii < UPLOADSHARDTHREAD; ii++) {
                queue.add(new UploadShard());
            }
        }
        return queue;
    }

    static void startUploadShard(UploadBlock uploadBlock, Shard shard, int shardId) throws InterruptedException {
        UploadShard uploader = getQueue().take();
        uploader.shard = shard;
        uploader.uploadBlock = uploadBlock;
        uploader.shardId = shardId;
        GlobleThreadPool.execute(uploader);
    }

    private UploadBlock uploadBlock;
    private Shard shard;
    private int shardId;

    private UploadShardReq makeUploadShardReq(PreAllocNodeStat node) {
        UploadShardReq req = new UploadShardReq();
        req.setBPDID(UserConfig.superNode.getId());
        req.setBPDSIGN(node.getSign().getBytes());
        req.setUSERSIGN(uploadBlock.signArg.getBytes());
        req.setDAT(shard.getData());
        req.setSHARDID(shardId);
        req.setVHF(shard.getVHF());
        return req;
    }

    @Override
    public void run() {
        try {
            UploadShardRes res = new UploadShardRes();
            res.setSHARDID(shardId);
            res.setVHF(shard.getVHF());
            PreAllocNodeStat node = uploadBlock.excessNode.poll();
            if (node == null) {
                res.setDNSIGN(null);
                uploadBlock.onResponse(res);
                return;
            }
            while (true) {
                res.setNODEID(node.getId());
                UploadShardReq req = this.makeUploadShardReq(node);
                long l = System.currentTimeMillis();
                long ctrtimes = 0;
                try {
                    GetNodeCapacityReq ctlreq = new GetNodeCapacityReq();
                    ctlreq.setRetryTimes(uploadBlock.retryTimes);
                    ctlreq.setStartTime(uploadBlock.sTime);
                    GetNodeCapacityResp ctlresp = (GetNodeCapacityResp) P2PUtils.requestNode(ctlreq, node.getNode(), uploadBlock.VNU.toString());
                    req.setAllocId(ctlresp.getAllocId());
                    ctrtimes = System.currentTimeMillis() - l;
                    if (!ctlresp.isWritable()) {
                        LOG.warn("[" + uploadBlock.VNU + "]Node " + node.getId() + " is unavailabe,take times " + ctrtimes + " ms");
                        node.setERR();
                        PreAllocNodeStat n = uploadBlock.excessNode.poll();
                        if (n == null) {
                            res.setDNSIGN(null);
                            break;
                        } else {
                            node = n;
                            continue;
                        }
                    }
                    if (ctlresp.getAllocId() == null || ctlresp.getAllocId().trim().isEmpty()) {
                        LOG.warn("[" + uploadBlock.VNU + "]Node " + node.getId() + ",AllocId is null");
                    }
                    UploadShard2CResp resp = (UploadShard2CResp) P2PUtils.requestNode(req, node.getNode(), uploadBlock.VNU.toString());
                    long times = System.currentTimeMillis() - l;
                    if (resp.getRES() == UploadShardRes.RES_OK || resp.getRES() == UploadShardRes.RES_VNF_EXISTS) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("[" + uploadBlock.VNU + "]Upload OK:" + shardId + "/"
                                    + Base58.encode(req.getVHF()) + " to " + node.getId() + ",RES:"
                                    + resp.getRES() + ",take times " + ctrtimes + "/" + times + " ms");
                        }
                        node.setOK(times);
                        if (resp.getDNSIGN() == null || resp.getDNSIGN().trim().isEmpty()) {
                            res.setDNSIGN("exists");
                        } else {
                            res.setDNSIGN(resp.getDNSIGN());
                        }
                        break;
                    } else {
                        node.setERR();
                        PreAllocNodeStat n = uploadBlock.excessNode.poll();
                        if (n == null) {
                            LOG.error("[" + uploadBlock.VNU + "]Upload ERR:" + shardId + "/"
                                    + Base58.encode(req.getVHF()) + " to " + node.getId() + ",RES:"
                                    + resp.getRES() + ",take times " + ctrtimes + "/" + times + " ms");
                            res.setDNSIGN(null);
                            break;
                        } else {
                            LOG.error("[" + uploadBlock.VNU + "]Upload ERR:" + shardId + "/"
                                    + Base58.encode(req.getVHF()) + " to " + node.getId() + ",RES:"
                                    + resp.getRES() + ",take times " + ctrtimes + "/" + times
                                    + " ms,retry node " + n.getId());
                            node = n;
                        }
                    }
                } catch (Throwable ex) {
                    node.setERR();
                    PreAllocNodeStat n = uploadBlock.excessNode.poll();
                    if (n == null) {
                        LOG.error("[" + uploadBlock.VNU + "]Upload ERR:" + shardId + "/"
                                + Base58.encode(req.getVHF()) + " to " + node.getId() + ",take times " + ctrtimes + "/" + (System.currentTimeMillis() - l) + " ms");
                        res.setDNSIGN(null);
                        break;
                    } else {
                        LOG.error("[" + uploadBlock.VNU + "]Upload ERR:" + shardId + "/"
                                + Base58.encode(req.getVHF()) + " to " + node.getId()
                                + ",take times " + ctrtimes + "/" + (System.currentTimeMillis() - l) + " ms,retry node " + n.getId());
                        node = n;
                    }
                }
            }
            uploadBlock.onResponse(res);
        } finally {
            getQueue().add(this);
        }
    }
}
