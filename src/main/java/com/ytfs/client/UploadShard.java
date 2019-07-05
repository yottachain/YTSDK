package com.ytfs.client;

import com.ytfs.service.packet.UploadShardRes;
import static com.ytfs.service.packet.UploadShardRes.RES_NETIOERR;
import static com.ytfs.common.conf.UserConfig.UPLOADSHARDTHREAD;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadShard2CResp;
import com.ytfs.service.packet.UploadShardReq;
import com.ytfs.common.GlobleThreadPool;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.core.vo.Node;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class UploadShard implements Runnable {

    private static final Logger LOG = Logger.getLogger(UploadShard.class);
    private static final ArrayBlockingQueue<UploadShard> queue;

    static {
        int num = UPLOADSHARDTHREAD > 255 ? 255 : UPLOADSHARDTHREAD;
        num = num < 5 ? 5 : num;
        queue = new ArrayBlockingQueue(num);
        for (int ii = 0; ii < num; ii++) {
            queue.add(new UploadShard());
        }
    }

    static void startUploadShard(UploadShardReq req, ShardNode node, UploadBlock uploadBlock, ObjectId VNU) throws InterruptedException {
        UploadShard uploader = queue.take();
        uploader.node = node.getNode();
        uploader.req = req;
        uploader.uploadBlock = uploadBlock;
        uploader.VNU = VNU;
        GlobleThreadPool.execute(uploader);
    }

    private UploadShardReq req;
    private Node node;
    private UploadBlock uploadBlock;
    private ObjectId VNU;

    @Override
    public void run() {
        try {
            UploadShardRes res = new UploadShardRes();
            res.setSHARDID(req.getSHARDID());
            res.setNODEID(node.getId());
            try {
                UploadShard2CResp resp = (UploadShard2CResp) P2PUtils.requestNode(req, node, VNU.toString());
                res.setRES(resp.getRES());
                if (LOG.isDebugEnabled()) {
                    if (resp.getRES() == UploadShardRes.RES_OK || resp.getRES() == UploadShardRes.RES_VNF_EXISTS) {
                        LOG.debug("[" + VNU + "]Upload OK:" + req.getVBI() + "/(" + req.getSHARDID() + ")" + Base58.encode(req.getVHF()) + " to " + node.getId() + ",RES:" + resp.getRES());
                    }else{
                        LOG.error("[" + VNU + "]Upload ERR:" + req.getVBI() + "/(" + req.getSHARDID() + ")" + Base58.encode(req.getVHF()) + " to " + node.getId() + ",RES:" + resp.getRES());
                    }
                }
            } catch (Throwable ex) {
                res.setRES(RES_NETIOERR);
                if (LOG.isDebugEnabled()) {
                    LOG.error("[" + VNU + "]Upload ERR:" + req.getVBI() + "/(" + req.getSHARDID() + ")" + Base58.encode(req.getVHF()) + " to " + node.getId());
                }
            }
            uploadBlock.onResponse(res);
        } finally {
            queue.add(this);
        }
    }
}
