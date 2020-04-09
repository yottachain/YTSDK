package com.ytfs.client.batch;

import com.ytfs.client.ErrorNodeCache;
import com.ytfs.client.PreAllocNodeStat;
import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.codec.Shard;
import com.ytfs.common.conf.UserConfig;
import static com.ytfs.common.conf.UserConfig.UPLOADSHARDTHREAD;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.tracing.GlobalTracer;
import com.ytfs.service.packet.UploadFackShardReq;
import com.ytfs.service.packet.UploadShard2CResp;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.service.packet.node.GetNodeCapacityReq;
import com.ytfs.service.packet.node.GetNodeCapacityResp;
import io.jafka.jeos.util.Base58;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;
import org.tanukisoftware.wrapper.WrapperManager;

public class UploadFackShard implements Runnable {

    static int NODE_SLEEP = 100;

    static {
        String num = WrapperManager.getProperties().getProperty("wrapper.batch.node.sleep", "10");
        try {
            NODE_SLEEP = Integer.parseInt(num);
        } catch (Exception d) {
        }
    }

    private static final Logger LOG = Logger.getLogger(UploadFackShard.class);
    private static ArrayBlockingQueue<UploadFackShard> queue = null;

    public static void init() {
        if (queue == null) {
            queue = new ArrayBlockingQueue(UPLOADSHARDTHREAD);
            for (int ii = 0; ii < UPLOADSHARDTHREAD; ii++) {
                queue.add(new UploadFackShard());
            }
        }
    }

    static void startUploadShard(UploadFackBlock uploadBlock, Shard shard, int shardId) throws InterruptedException {
        UploadFackShard uploader = queue.take();
        uploader.shard = shard;
        uploader.uploadBlock = uploadBlock;
        uploader.shardId = shardId;
        uploader.logHead = "[" + uploadBlock.VNU + "][" + uploadBlock.id + "][" + shardId + "]";
        GlobleThreadPool.execute(uploader);
    }

    private UploadFackBlock uploadBlock;
    private Shard shard;
    private int shardId;
    private String logHead;

    private UploadFackShardReq makeUploadShardReq(PreAllocNodeStat node) {
        UploadFackShardReq req = new UploadFackShardReq();
        req.setBPDID(UserConfig.superNode.getId());
        req.setBPDSIGN(node.getSign().getBytes());
        req.setUSERSIGN(uploadBlock.signArg.getBytes());
        req.setDAT(shard.getData());
        req.setSHARDID(shardId);
        req.setVHF(shard.getVHF());
        req.setSleep(NODE_SLEEP);
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
                UploadFackShardReq req = this.makeUploadShardReq(node);
                long l = System.currentTimeMillis();
                long ctrtimes = 0;
                try {
                    GetNodeCapacityReq ctlreq = new GetNodeCapacityReq();
                    ctlreq.setRetryTimes(uploadBlock.retryTimes);
                    ctlreq.setStartTime(uploadBlock.sTime);
                    GetNodeCapacityResp ctlresp = (GetNodeCapacityResp) P2PUtils.requestNode(ctlreq, node.getNode(), logHead);
                    req.setAllocId(ctlresp.getAllocId());
                    ctrtimes = System.currentTimeMillis() - l;
                    if (!ctlresp.isWritable()) {
                        LOG.warn(logHead + "Node " + node.getId() + " is busy,take times " + ctrtimes + " ms");
                        node.setBusy();
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
                        LOG.warn(logHead + "Node " + node.getId() + ",AllocId is null");
                    }
                    UploadShard2CResp resp;
                    Tracer tracer = GlobalTracer.getTracer();
                    if (tracer != null) {
                        Span span = tracer.buildSpan("SendShard").start();
                        try (Scope scope = tracer.scopeManager().activate(span)) {
                            resp = (UploadShard2CResp) P2PUtils.requestNode(req, node.getNode(), logHead);
                        } catch (Throwable ex) {
                            Tags.ERROR.set(span, true);
                            throw ex;
                        } finally {
                            span.finish();
                        }
                    } else {
                        //LOG.info("发送："+req.getClass().getName());
                        Object obj = P2PUtils.requestNode(req, node.getNode(), logHead);
                        //LOG.info("返回："+obj.getClass().getName());
                        resp = (UploadShard2CResp) obj;// P2PUtils.requestNode(req, node.getNode(), logHead);
                    }
                    long times = System.currentTimeMillis() - l;
                    if (resp.getRES() == UploadShardRes.RES_OK || resp.getRES() == UploadShardRes.RES_VNF_EXISTS) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(logHead + "Upload OK:" + Base58.encode(req.getVHF()) + " to " + node.getId() + ",RES:"
                                    + resp.getRES() + ",take times " + ctrtimes + "/" + times + " ms");
                        }
                        node.setOK(times);
                        if (resp.getDNSIGN() == null || resp.getDNSIGN().trim().isEmpty()) {
                            res.setDNSIGN("exists");
                        } else {
                            res.setDNSIGN(resp.getDNSIGN());
                        }
                        if (!shard.isCopyShard()) {
                            shard.clearData();
                        }
                        break;
                    } else {
                        if (resp.getRES() == UploadShardRes.RES_NO_SPACE) {
                            ErrorNodeCache.addErrorNode(node.getId());
                        }
                        node.setERR();
                        PreAllocNodeStat n = uploadBlock.excessNode.poll();
                        if (n == null) {
                            LOG.error(logHead + "Upload ERR:" + Base58.encode(req.getVHF()) + " to " + node.getId() + ",RES:"
                                    + resp.getRES() + ",take times " + ctrtimes + "/" + times + " ms");
                            res.setDNSIGN(null);
                            break;
                        } else {
                            LOG.error(logHead + "Upload ERR:" + Base58.encode(req.getVHF()) + " to " + node.getId() + ",RES:"
                                    + resp.getRES() + ",take times " + ctrtimes + "/" + times + " ms,retry node " + n.getId());
                            node = n;
                        }
                    }
                } catch (Throwable ex) {
                    node.setERR();
                    PreAllocNodeStat n = uploadBlock.excessNode.poll();
                    if (n == null) {
                        LOG.error(logHead + "Upload ERR:" + Base58.encode(req.getVHF()) + " to " + node.getId()
                                + ",take times " + ctrtimes + "/" + (System.currentTimeMillis() - l) + " ms");
                        res.setDNSIGN(null);
                        break;
                    } else {
                        LOG.error(logHead + "Upload ERR:" + Base58.encode(req.getVHF()) + " to " + node.getId()
                                + ",take times " + ctrtimes + "/" + (System.currentTimeMillis() - l) + " ms,retry node " + n.getId());
                        node = n;
                    }
                }
            }
            uploadBlock.onResponse(res);
        } finally {
            queue.add(this);
        }
    }
}
