package com.ytfs.client;

import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.codec.Shard;
import com.ytfs.common.conf.UserConfig;
import static com.ytfs.common.conf.UserConfig.UPLOADSHARDTHREAD;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadShard2CResp;
import com.ytfs.service.packet.UploadShardReq;
import com.ytfs.service.packet.UploadShardRes;
import static com.ytfs.service.packet.UploadShardRes.RES_CACHE_FILL;
import static com.ytfs.service.packet.UploadShardRes.RES_NETIOERR;
import com.ytfs.service.packet.node.GetNodeCapacityReq;
import com.ytfs.service.packet.node.GetNodeCapacityResp;
import io.jafka.jeos.util.Base58;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
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

    static boolean startUploadShard(UploadBlock uploadBlock, ShardNode node, Shard shard) throws InterruptedException {
        UploadShard uploader = getQueue().poll(15, TimeUnit.SECONDS);
        if (uploader == null) {
            return false;
        }
        uploader.node = node;
        uploader.shard = shard;
        uploader.uploadBlock = uploadBlock;
        uploader.shardId = node.getShardid();
        GlobleThreadPool.execute(uploader);
        return true;
    }

    private UploadBlock uploadBlock;
    private ShardNode node;
    private Shard shard;
    private int shardId;

    private UploadShardReq makeUploadShardReq() {
        UploadShardReq req = new UploadShardReq();
        req.setBPDID(uploadBlock.bpdNode.getId());
        req.setBPDSIGN(node.getSign());
        req.setDAT(shard.getData());
        req.setSHARDID(shardId);
        req.setVBI(uploadBlock.VBI);
        req.setVHF(shard.getVHF());
        sign(req, node.getNodeId());
        return req;
    }

    private void sign(UploadShardReq req, int nodeid) {
        ByteBuffer bs = ByteBuffer.allocate(48);
        bs.put(req.getVHF());
        bs.putInt(req.getSHARDID());
        bs.putInt(nodeid);
        bs.putLong(req.getVBI());
        bs.flip();
        byte[] sign = KeyStoreCoder.ecdsaSign(bs.array(), UserConfig.privateKey);
        req.setUSERSIGN(sign);
        //LOG.info(req.getSHARDID() + " getUSERSIGN " + Hex.encodeHexString(req.getBPDSIGN()));
    }

    @Override
    public void run() {
        try {
            UploadShardRes res = new UploadShardRes();
            res.setSHARDID(shardId);
            while (true) {
                UploadShardReq req = this.makeUploadShardReq();
                res.setNODEID(node.getNodeId());
                long l = System.currentTimeMillis();
                long ctrtimes = 0;
                try {
                    GetNodeCapacityReq ctlreq = new GetNodeCapacityReq();
                    GetNodeCapacityResp ctlresp = (GetNodeCapacityResp) P2PUtils.requestNode(ctlreq, node.getNode(), uploadBlock.VNU.toString());
                    req.setAllocId(ctlresp.getAllocId());
                    ctrtimes = System.currentTimeMillis() - l;
                    if (!ctlresp.isWritable()) {
                        LOG.warn("[" + uploadBlock.VNU + "]Node " + node.getNodeId() + " is unavailabe,take times " + ctrtimes + " ms");
                        ShardNode n = uploadBlock.excessNode.poll();
                        if (n == null) {
                            res.setRES(RES_NETIOERR);
                            break;
                        } else {
                            node = n;
                            continue;
                        }
                    }
                    if (ctlresp.getAllocId() == null || ctlresp.getAllocId().trim().isEmpty()) {
                        LOG.warn("[" + uploadBlock.VNU + "]Node " + node.getNodeId() + ",AllocId is null");
                    }
                    UploadShard2CResp resp = (UploadShard2CResp) P2PUtils.requestNode(req, node.getNode(), uploadBlock.VNU.toString());
                    res.setRES(resp.getRES());
                    if (resp.getRES() == UploadShardRes.RES_OK || resp.getRES() == UploadShardRes.RES_VNF_EXISTS) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("[" + uploadBlock.VNU + "]Upload OK:" + req.getVBI() + "/(" + shardId + ")"
                                    + Base58.encode(req.getVHF()) + " to " + node.getNodeId() + ",RES:"
                                    + resp.getRES() + ",take times " + ctrtimes + "/" + (System.currentTimeMillis() - l) + " ms");
                        }
                        break;
                    } else {
                        ShardNode n = uploadBlock.excessNode.poll();
                        if (n == null) {
                            LOG.error("[" + uploadBlock.VNU + "]Upload ERR:" + req.getVBI() + "/(" + shardId + ")"
                                    + Base58.encode(req.getVHF()) + " to " + node.getNodeId() + ",RES:"
                                    + resp.getRES() + ",take times " + ctrtimes + "/" + (System.currentTimeMillis() - l) + " ms");
                            if (RES_CACHE_FILL == resp.getRES()) {
                                res.setRES(RES_NETIOERR);
                            }
                            break;
                        } else {
                            LOG.error("[" + uploadBlock.VNU + "]Upload ERR:" + req.getVBI() + "/(" + shardId + ")"
                                    + Base58.encode(req.getVHF()) + " to " + node.getNodeId() + ",RES:"
                                    + resp.getRES() + ",take times " + ctrtimes + "/" + (System.currentTimeMillis() - l)
                                    + " ms,retry node " + n.getNodeId());
                            node = n;
                        }
                    }
                } catch (Throwable ex) {
                    res.setRES(RES_NETIOERR);
                    ShardNode n = uploadBlock.excessNode.poll();
                    if (n == null) {
                        LOG.error("[" + uploadBlock.VNU + "]Upload ERR:" + req.getVBI() + "/(" + shardId + ")"
                                + Base58.encode(req.getVHF()) + " to " + node.getNodeId() + ",take times " + ctrtimes + "/" + (System.currentTimeMillis() - l) + " ms");
                        break;
                    } else {
                        LOG.error("[" + uploadBlock.VNU + "]Upload ERR:" + req.getVBI() + "/(" + shardId + ")"
                                + Base58.encode(req.getVHF()) + " to " + node.getNodeId()
                                + ",take times " + ctrtimes + "/" + (System.currentTimeMillis() - l) + " ms,retry node " + n.getNodeId());
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
