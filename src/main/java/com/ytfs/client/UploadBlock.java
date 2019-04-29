package com.ytfs.client;

import com.ytfs.service.UserConfig;
import com.ytfs.service.packet.UploadShardRes;
import static com.ytfs.service.packet.UploadShardRes.RES_OK;
import com.ytfs.service.codec.Block;
import com.ytfs.service.codec.BlockAESEncryptor;
import com.ytfs.service.packet.UploadShardReq;
import com.ytfs.service.codec.KeyStoreCoder;
import com.ytfs.service.codec.Shard;
import com.ytfs.service.codec.ShardRSEncoder;
import com.ytfs.service.net.P2PUtils;
import static com.ytfs.service.packet.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadBlockEndReq;
import com.ytfs.service.packet.UploadBlockSubReq;
import com.ytfs.service.packet.UploadBlockSubResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class UploadBlock {

    private static final Logger LOG = Logger.getLogger(UploadBlock.class);
    private ShardRSEncoder rs;
    private final Block block;
    private final short id;
    private final ShardNode[] nodes;
    private final long VBI;
    private final SuperNode bpdNode;
    private final List<UploadShardRes> resList = new ArrayList();
    private final Map<Integer, Shard> map = new HashMap();

    public UploadBlock(Block block, short id, ShardNode[] nodes, long VBI, SuperNode bpdNode) {
        this.block = block;
        this.id = id;
        this.nodes = nodes;
        this.VBI = VBI;
        this.bpdNode = bpdNode;
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
            LOG.info("Download shardcount " + len + ",take time " + (System.currentTimeMillis() - l) + "ms");
        } catch (Exception r) {
            LOG.error("", r);
            throw new ServiceException(SERVER_ERROR);
        }
    }

    private void completeUploadBlock(byte[] ks) throws ServiceException {
        UploadBlockEndReq req = new UploadBlockEndReq();
        req.setId(id);
        req.setVBI(VBI);
        req.setVHP(block.getVHP());
        req.setVHB(rs.makeVHB());
        req.setKEU(KeyStoreCoder.rsaEncryped(ks, UserConfig.KUEp));
        req.setKED(KeyStoreCoder.encryped(ks, block.getKD()));
        req.setOriginalSize(block.getOriginalSize());
        req.setRealSize(block.getRealSize());
        req.setRsShard(rs.getShardList().get(0).isRsShard());
        P2PUtils.requestBPU(req, bpdNode);
        LOG.info("Upload block " + id + ",VBI:" + VBI);
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
            UploadShard.startUploadShard(req, n, this);
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
        while (true) {
            UploadBlockSubReq uloadBlockSubReq = doUploadShardRes();
            if (uloadBlockSubReq == null) {
                return;
            } else {
                if (retrycount >= 5) {
                    throw new ServiceException(SERVER_ERROR);
                }
            }
            UploadBlockSubResp resp = (UploadBlockSubResp) P2PUtils.requestBPU(uloadBlockSubReq, bpdNode);
            if (resp.getNodes() == null || resp.getNodes().length == 0) {
                throw new ServiceException(SERVER_ERROR);
            }
            secondUpload(resp);
            retrycount++;
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
            UploadShard.startUploadShard(req, n, this);
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
        req.setUSERSIGN(new byte[0]);
        // Key key = KeyStoreCoder.rsaPrivateKey(UserConfig.KUSp);
        /*
        try {
            Signature signet = java.security.Signature.getInstance("DSA");
            signet.initSign((PrivateKey) key);
            ByteBuffer bs = ByteBuffer.allocate(48);
            bs.put(req.getVHF());
            bs.putInt(req.getSHARDID());
            bs.putInt(nodeid);
            bs.putLong(req.getVBI());
            bs.flip();
            signet.update(bs.array());
            req.setUSERSIGN(signet.sign());
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }*/
    }
}
