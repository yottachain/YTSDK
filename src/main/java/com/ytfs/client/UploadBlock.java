package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.service.packet.UploadShardRes;
import static com.ytfs.service.packet.UploadShardRes.RES_OK;
import com.ytfs.common.codec.Block;
import com.ytfs.common.codec.BlockAESEncryptor;
import com.ytfs.service.packet.UploadShardReq;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.codec.Shard;
import com.ytfs.common.codec.ShardRSEncoder;
import com.ytfs.common.net.P2PUtils;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadBlockEndReq;
import com.ytfs.service.packet.UploadBlockSubReq;
import com.ytfs.service.packet.UploadBlockSubResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class UploadBlock {

    private static final Logger LOG = Logger.getLogger(UploadBlock.class);
    private ShardRSEncoder rs;
    private final Block block;
    private final short id;
    private final ShardNode[] nodes;
    private final long VBI;
    private final ObjectId VNU;
    private final SuperNode bpdNode;
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
            completeUploadBlock(ks);
            LOG.info("[" + VNU + "]Upload shardcount " + len + ",take time " + (System.currentTimeMillis() - l) + "ms");
        } catch (Exception r) {
            throw r instanceof ServiceException ? (ServiceException) r : new ServiceException(SERVER_ERROR);
        }
    }

    private void completeUploadBlock(byte[] ks) throws ServiceException {
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
        LOG.info("[" + VNU + "]Upload block " + id + "/" + VBI + " OK.");
    }

    private void firstUpload() throws InterruptedException {
        List<Shard> shards = rs.getShardList();
        int nodeindex = 0;
        for (Shard sd : shards) {
            map.put(nodeindex, sd);
            ShardNode n = nodes[nodeindex];
            UploadShardReq req = new UploadShardReq();
            req.setBPDID(bpdNode.getId());
            req.setBPDSIGN(n.getSign());
            req.setDAT(sd.getData());
            req.setSHARDID(nodeindex);
            req.setVBI(VBI);
            req.setVHF(sd.getVHF());
            sign(req, nodes[nodeindex].getNodeId());
            UploadShard.startUploadShard(req, n, this, VNU);
            nodeindex++;
        }
        synchronized (this) {
            while (resList.size() != shards.size()) {
                this.wait(1000 * 15);
            }
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
                if (retrycount >= 5) {
                    LOG.error("[" + VNU + "]Upload block " + id + "/" + VBI + " 5 retries were unsuccessful.");
                    throw new ServiceException(SERVER_ERROR);
                }
            }
            UploadBlockSubResp resp = (UploadBlockSubResp) P2PUtils.requestBPU(uloadBlockSubReq, bpdNode, VNU.toString());
            if (resp.getNodes() == null || resp.getNodes().length == 0) {//OK
                break;
            }
            LOG.info("[" + VNU + "]Upload block " + id + "/" + VBI + " retrying,remaining " + resp.getNodes().length + " shards.");
            secondUpload(resp);
        }
    }

    private void secondUpload(UploadBlockSubResp resp) throws InterruptedException {
        ShardNode[] shardNodes = resp.getNodes();
        for (ShardNode n : shardNodes) {
            Shard sd = map.get(n.getShardid());
            UploadShardReq req = new UploadShardReq();
            req.setBPDID(bpdNode.getId());
            req.setBPDSIGN(n.getSign());
            req.setDAT(sd.getData());
            req.setSHARDID(n.getShardid());
            req.setVBI(VBI);
            req.setVHF(sd.getVHF());
            sign(req, n.getNodeId());
            UploadShard.startUploadShard(req, n, this, VNU);
        }
        synchronized (this) {
            while (resList.size() != shardNodes.length) {
                this.wait(1000 * 15);
            }
        }
    }

    private UploadBlockSubReq doUploadShardRes() {
        List<UploadShardRes> ls = new ArrayList();
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
        if (ls.isEmpty()) {
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
}
