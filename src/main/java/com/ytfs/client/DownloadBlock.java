package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.codec.BlockAESDecryptor;
import com.ytfs.common.codec.BlockEncrypted;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.common.codec.Shard;
import com.ytfs.common.codec.erasure.ShardRSDecoder;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.user.DownloadBlockDBResp;
import com.ytfs.service.packet.user.DownloadBlockInitReq;
import com.ytfs.service.packet.user.DownloadBlockInitResp;
import com.ytfs.service.packet.DownloadShardReq;
import com.ytfs.service.packet.DownloadShardResp;
import static com.ytfs.common.ServiceErrorCode.INVALID_SHARD;
import com.ytfs.common.ServiceException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import static com.ytfs.common.ServiceErrorCode.COMM_ERROR;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.codec.ShardEncoder;
import com.ytfs.common.codec.lrc.ShardLRCDecoder;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DownloadBlock {

    private static final Logger LOG = Logger.getLogger(DownloadBlock.class);
    protected final ObjectRefer refer;
    private byte[] data;
    private final List<DownloadShardResp> resList = new ArrayList();
    private byte[] ks;

    DownloadBlock(ObjectRefer refer) throws ServiceException {
        this.refer = refer;
    }

    public byte[] getData() {
        return data;
    }

    public void load() throws ServiceException {
        ks = KeyStoreCoder.aesDecryped(refer.getKEU(), UserConfig.AESKey);
        long l = System.currentTimeMillis();
        DownloadBlockInitReq req = new DownloadBlockInitReq();
        req.setVBI(refer.getVBI());
        SuperNode pbd = SuperNodeList.getSuperNode(refer.getSuperID());
        Object resp = P2PUtils.requestBPU(req, pbd, UserConfig.SN_RETRYTIMES);
        LOG.info("[" + refer.getVBI() + "]Download init OK at sn" + refer.getSuperID() + ",take times " + (System.currentTimeMillis() - l) + "ms");
        if (resp instanceof DownloadBlockDBResp) {
            this.data = aesDBDecode(((DownloadBlockDBResp) resp).getData());
            LOG.debug("[" + refer.getVBI() + "]Download block " + refer.getId() + " from DB.");
        } else {
            DownloadBlockInitResp initresp = (DownloadBlockInitResp) resp;
            switch (initresp.getAR()) {
                case ShardEncoder.AR_COPY_MODE:
                    this.data = loadCopyShard(initresp);
                    LOG.info("[" + refer.getVBI() + "]Download block " + refer.getId() + " copy.");
                    break;
                case ShardEncoder.AR_RS_MODE:
                    try {
                        this.data = loadRSShard(initresp);
                        LOG.info("[" + refer.getVBI() + "]Download block " + refer.getId() + " RS shards.");
                    } catch (InterruptedException e) {
                        throw new ServiceException(SERVER_ERROR, e.getMessage());
                    }
                    break;
                default:
                    try {
                        this.data = loadLRCShard(initresp);
                        LOG.info("[" + refer.getVBI() + "]Download block " + refer.getId() + " RS shards.");
                    } catch (InterruptedException e) {
                        throw new ServiceException(SERVER_ERROR, e.getMessage());
                    }
                    break;
            }
        }
    }

    void onResponse(DownloadShardResp res) {
        synchronized (this) {
            resList.add(res);
            this.notify();
        }
    }

    private byte[] loadLRCShard(DownloadBlockInitResp initresp) throws InterruptedException, ServiceException {
        List<Shard> shards = new ArrayList();
        int len = initresp.getAR();
        ConcurrentLinkedQueue<DownloadShardParam> shardparams = new ConcurrentLinkedQueue();
        Map<Integer, Node> map = new HashMap();
        for (Node n : initresp.getNodes()) {
            map.put(n.getId(), n);
        }
        for (int ii = 0; ii < initresp.getNodeids().length; ii++) {
            DownloadShardParam param = new DownloadShardParam();
            param.setVHF(initresp.getVHF()[ii]);
            Node n = map.get(initresp.getNodeids()[ii]);
            if (n == null) {
                LOG.warn("[" + refer.getVBI() + "]Node Offline,ID:" + initresp.getNodeids()[ii]);
                continue;
            }
            param.setNode(n);
            shardparams.add(param);
        }
        long l = System.currentTimeMillis();
        for (int ii = 0; ii < len; ii++) {
            DownloadShard.startDownloadShard(shardparams, this);
        }
        synchronized (this) {
            while (resList.size() != len) {
                this.wait(1000 * 15);
            }
        }
        resList.stream().filter((res) -> (res.getData() != null)).forEachOrdered((res) -> {
            shards.add(new Shard(res.getData()));
        });
        resList.clear();
        ShardLRCDecoder lrc;
        try {
            BlockEncrypted be = new BlockEncrypted(refer.getRealSize());
            lrc = new ShardLRCDecoder(be.getEncryptedBlockSize());
            for (Shard shard : shards) {
                boolean b = lrc.addShard(shard.getData());
                if (b) {
                    break;
                }
            }
            if (lrc.isFinished()) {
                LOG.info("[" + refer.getVBI() + "]Download shardcount " + len + ",take times " + (System.currentTimeMillis() - l) + "ms");
                be = lrc.decode();
                BlockAESDecryptor dec = new BlockAESDecryptor(be.getData(), ks);
                dec.decrypt();
                return dec.getSrcData();
            } else {
                lrc.free();
                LOG.error("[" + refer.getVBI() + "]Download shardcount " + shards.size() + "/" + initresp.getVNF() + ",Not enough shards present.");
                throw new ServiceException(COMM_ERROR);
            }
        } catch (Throwable t) {
            throw t instanceof ServiceException ? (ServiceException) t : new ServiceException(SERVER_ERROR, t.getMessage());
        }
    }

    private byte[] loadRSShard(DownloadBlockInitResp initresp) throws InterruptedException, ServiceException {
        List<Shard> shards = new ArrayList();
        int len = initresp.getVNF() - UserConfig.Default_PND;
        ConcurrentLinkedQueue<DownloadShardParam> shardparams = new ConcurrentLinkedQueue();
        Map<Integer, Node> map = new HashMap();
        for (Node n : initresp.getNodes()) {
            map.put(n.getId(), n);
        }
        for (int ii = 0; ii < initresp.getNodeids().length; ii++) {
            DownloadShardParam param = new DownloadShardParam();
            param.setVHF(initresp.getVHF()[ii]);
            Node n = map.get(initresp.getNodeids()[ii]);
            if (n == null) {
                LOG.warn("[" + refer.getVBI() + "]Node Offline,ID:" + initresp.getNodeids()[ii]);
                continue;
            }
            param.setNode(n);
            shardparams.add(param);
        }
        long l = System.currentTimeMillis();
        for (int ii = 0; ii < len; ii++) {
            DownloadShard.startDownloadShard(shardparams, this);
        }
        synchronized (this) {
            while (resList.size() != len) {
                this.wait(1000 * 15);
            }
        }
        resList.stream().filter((res) -> (res.getData() != null)).forEachOrdered((res) -> {
            shards.add(new Shard(res.getData()));
        });
        resList.clear();
        if (shards.size() >= len) {
            LOG.info("[" + refer.getVBI() + "]Download shardcount " + len + ",take times " + (System.currentTimeMillis() - l) + "ms");
            BlockEncrypted be = new BlockEncrypted(refer.getRealSize());
            ShardRSDecoder rsdec = new ShardRSDecoder(shards, be.getEncryptedBlockSize());
            be = rsdec.decode();
            BlockAESDecryptor dec = new BlockAESDecryptor(be.getData(), ks);
            dec.decrypt();
            return dec.getSrcData();
        } else {
            LOG.error("[" + refer.getVBI() + "]Download shardcount " + shards.size() + "/" + initresp.getVNF() + ",Not enough shards present.");
            throw new ServiceException(COMM_ERROR);
        }
    }

    private byte[] loadCopyShard(DownloadBlockInitResp initresp) throws ServiceException {
        DownloadShardReq req = new DownloadShardReq();
        int len = initresp.getVNF();
        int count = initresp.getNodes().length;
        int index = 0;
        Map<Integer, Node> map = new HashMap();
        for (Node n : initresp.getNodes()) {
            map.put(n.getId(), n);
        }
        ServiceException t = null;
        while (index < len && index < count) {
            try {
                Node n = map.get(initresp.getNodeids()[index]);
                byte[] VHF = initresp.getVHF()[index];
                req.setVHF(VHF);
                DownloadShardResp resp = (DownloadShardResp) P2PUtils.requestNode(req, n);
                if (DownloadShard.verify(resp, VHF, refer.getVBI())) {
                    return aesCopyDecode(resp.getData());
                }
                index++;
            } catch (ServiceException e) {
                t = e;
            }
        }
        LOG.error("[" + refer.getVBI() + "]Download copy shardcount " + count + " ERR.");
        throw t == null ? new ServiceException(INVALID_SHARD) : t;
    }

    private byte[] aesCopyDecode(byte[] data) {
        BlockEncrypted be = new BlockEncrypted(refer.getRealSize());
        ShardRSDecoder rsdec = new ShardRSDecoder(new Shard(data), be.getEncryptedBlockSize());
        be = rsdec.decode();
        BlockAESDecryptor dec = new BlockAESDecryptor(be.getData(), ks);
        dec.decrypt();
        return dec.getSrcData();
    }

    private byte[] aesDBDecode(byte[] data) {
        BlockAESDecryptor dec = new BlockAESDecryptor(data, ks);
        dec.decrypt();
        return dec.getSrcData();
    }

}
