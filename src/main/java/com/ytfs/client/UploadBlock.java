package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.service.packet.UploadShardRes;
import static com.ytfs.service.packet.UploadShardRes.RES_OK;
import com.ytfs.common.codec.Block;
import com.ytfs.common.codec.BlockAESEncryptor;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.codec.Shard;
import com.ytfs.common.codec.ShardRSEncoder;
import com.ytfs.common.net.P2PUtils;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import static com.ytfs.common.conf.ServerConfig.Excess_Shard_Index;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadBlockEndReq;
import com.ytfs.service.packet.UploadBlockSubReq;
import com.ytfs.service.packet.UploadBlockSubResp;
import com.ytfs.service.packet.bp.ActiveCache;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class UploadBlock {

    private static final Logger LOG = Logger.getLogger(UploadBlock.class);
    private static int interval = 5000;
    private static int retrytimes = 5;

    static {
        try {
            interval = Integer.parseInt(System.getProperty("Dupload.sleep", "5000"));
        } catch (Exception r) {
        }
        try {
            retrytimes = Integer.parseInt(System.getProperty("Dupload.retry", "5"));
        } catch (Exception r) {
        }
    }

    private ShardRSEncoder rs;
    private final Block block;
    private final short id;
    private final ShardNode[] nodes;
    protected final long VBI;
    protected final ObjectId VNU;
    protected final SuperNode bpdNode;
    protected final ConcurrentLinkedQueue<ShardNode> excessNode = new ConcurrentLinkedQueue();
    private final List<UploadShardRes> resList = new ArrayList();
    private final Map<Integer, Shard> map = new HashMap();

    public UploadBlock(Block block, short id, ShardNode[] nodes, long VBI, SuperNode bpdNode, ObjectId VNU) {
        this.block = block;
        this.id = id;
        this.nodes = nodes;
        this.VBI = VBI;
        this.bpdNode = bpdNode;
        this.VNU = VNU;
    }

    void onResponse(UploadShardRes res) {
        synchronized (this) {
            resList.add(res);
            this.notify();
        }
    }

    void upload() throws ServiceException, InterruptedException {
        try {
            long l = System.currentTimeMillis();
            byte[] ks = KeyStoreCoder.generateRandomKey();
            BlockAESEncryptor aes = new BlockAESEncryptor(block, ks);
            aes.encrypt();
            rs = new ShardRSEncoder(aes.getBlockEncrypted());
            rs.encode();
            int len = rs.getShardList().size();
            firstUpload();
            subUpload();
            LOG.info("[" + VNU + "]Upload block " + id + "/" + VBI + ",shardcount " + len + ",take times " + (System.currentTimeMillis() - l) + "ms");
            completeUploadBlock(ks);
        } catch (Exception r) {
            throw r instanceof ServiceException ? (ServiceException) r : new ServiceException(SERVER_ERROR);
        }
    }

    private void completeUploadBlock(byte[] ks) throws ServiceException {
        long l = System.currentTimeMillis();
        UploadBlockEndReq req = new UploadBlockEndReq();
        req.setId(id);
        req.setVBI(VBI);
        req.setVHP(block.getVHP());
        req.setVHB(rs.makeVHB());
        req.setKEU(KeyStoreCoder.aesEncryped(ks, UserConfig.AESKey));
        req.setKED(KeyStoreCoder.aesEncryped(ks, block.getKD()));
        req.setOriginalSize(block.getOriginalSize());
        req.setRealSize(block.getRealSize());
        req.setRsShard(rs.getShardList().get(0).isRsShard());
        P2PUtils.requestBPU(req, bpdNode, VNU.toString());
        LOG.info("[" + VNU + "]Upload block " + id + "/" + VBI + " OK,take times " + (System.currentTimeMillis() - l) + "ms");
    }

    private void firstUpload() throws InterruptedException {
        for (ShardNode node : nodes) {
            if (node.getShardid() == Excess_Shard_Index) {
                excessNode.add(node);
            }
        }
        List<Shard> shards = rs.getShardList();
        int nodeindex = 0;
        for (Shard sd : shards) {
            map.put(nodeindex, sd);
            UploadShard.startUploadShard(this, nodes[nodeindex], sd);
            nodeindex++;
        }
        long times = 0;
        synchronized (this) {
            while (resList.size() != shards.size()) {
                this.wait(15000);
                times = times + 15000;
                if (times >= 60000) {
                    sendActive();
                }
            }
        }
    }

    private void sendActive() {
        try {
            ActiveCache active = new ActiveCache();
            active.setVBI(VBI);
            P2PUtils.requestBPU(active, bpdNode, VNU.toString());
        } catch (Exception r) {
        }
    }

    private void subUpload() throws InterruptedException, ServiceException {
        int retrycount = 0;
        int lasterrnum = 0;
        while (true) {
            UploadBlockSubReq uloadBlockSubReq = doUploadShardRes();
            if (uloadBlockSubReq == null) {
                return;
            } else {
                int errnum = uloadBlockSubReq.getRes().length;
                if (errnum != lasterrnum) {
                    retrycount = 0;
                    lasterrnum = errnum;
                } else {
                    retrycount++;
                }
                if (retrycount >= retrytimes) {
                    LOG.error("[" + VNU + "]Upload block " + id + "/" + VBI + " " + retrytimes + " retries were unsuccessful.");
                    throw new ServiceException(SERVER_ERROR);
                }
                try {
                    Thread.sleep(interval);
                } catch (Exception r) {
                }
            }
            UploadBlockSubResp resp = (UploadBlockSubResp) P2PUtils.requestBPU(uloadBlockSubReq, bpdNode, VNU.toString());
            if (resp.getNodes() == null || resp.getNodes().length == 0) {//OK
                break;
            }
            secondUpload(resp);
        }
    }

    private void secondUpload(UploadBlockSubResp resp) throws InterruptedException {
        excessNode.clear();
        ShardNode[] respNodes = resp.getNodes();
        for (ShardNode node : respNodes) {
            if (node.getShardid() == Excess_Shard_Index) {
                excessNode.add(node);
            }
        }
        int errcount = resp.getNodes().length - excessNode.size();
        LOG.info("[" + VNU + "]Upload block " + id + "/" + VBI + " retrying,remaining " + errcount + " shards.");
        for (int ii = 0; ii < errcount; ii++) {
            ShardNode node = resp.getNodes()[ii];
            Shard shard = map.get(node.getShardid());
            UploadShard.startUploadShard(this, node, shard);
        }
        long times = 0;
        synchronized (this) {
            while (resList.size() != errcount) {
                this.wait(1000 * 15);
                times = times + 15000;
                if (times >= 60000) {
                    sendActive();
                }
            }
        }
    }

    private UploadBlockSubReq doUploadShardRes() {
        List<UploadShardRes> ls = new ArrayList();
        ls.add(UploadShardRes.NeedExcessNodeSign);
        for (UploadShardRes res : resList) {
            if (res.getRES() != RES_OK) {
                if (res.getRES() != UploadShardRes.RES_VNF_EXISTS) {// RES_NO_SPACE RES_VNF_EXISTS 
                    ls.add(res);
                    if (res.getRES() == UploadShardRes.RES_NO_SPACE) {
                        LOG.info("[" + VNU + "]ERR 'NO_SPACE',id:" + res.getNODEID());
                    }
                }
            }
        }
        resList.clear();
        if (ls.size() == 1) {
            return null;
        } else {
            UploadShardRes[] ress = new UploadShardRes[ls.size()];
            ress = ls.toArray(ress);
            UploadBlockSubReq subreq = new UploadBlockSubReq();
            subreq.setRes(ress);
            subreq.setVBI(VBI);
            return subreq;
        }
    }
}
