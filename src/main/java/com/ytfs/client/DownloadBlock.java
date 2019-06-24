package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.codec.BlockAESDecryptor;
import com.ytfs.common.codec.BlockEncrypted;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.common.codec.Shard;
import com.ytfs.common.codec.ShardRSDecoder;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.DownloadBlockDBResp;
import com.ytfs.service.packet.DownloadBlockInitReq;
import com.ytfs.service.packet.DownloadBlockInitResp;
import com.ytfs.service.packet.DownloadShardReq;
import com.ytfs.service.packet.DownloadShardResp;
import static com.ytfs.common.ServiceErrorCode.INTERNAL_ERROR;
import static com.ytfs.common.ServiceErrorCode.INVALID_SHARD;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class DownloadBlock {

    private static final Logger LOG = Logger.getLogger(DownloadBlock.class);
    private final ObjectRefer refer;
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
        DownloadBlockInitReq req = new DownloadBlockInitReq();
        req.setVBI(refer.getVBI());
        SuperNode pbd = SuperNodeList.getBlockSuperNode(refer.getSuperID());
        Object resp = P2PUtils.requestBPU(req, pbd);
        if (resp instanceof DownloadBlockDBResp) {
            this.data = aesDBDecode(((DownloadBlockDBResp) resp).getData());
        } else {
            DownloadBlockInitResp initresp = (DownloadBlockInitResp) resp;
            if (initresp.getVNF() < 0) {
                this.data = loadCopyShard(initresp);
            } else {
                try {
                    this.data = loadRSShard(initresp);
                } catch (InterruptedException e) {
                    throw new ServiceException(INTERNAL_ERROR, e.getMessage());
                }
            }
        }
        LOG.info("Download block " + refer.getId() + ",VBI:" + refer.getVBI());
    }

    void onResponse(DownloadShardResp res) {
        synchronized (this) {
            resList.add(res);
            this.notify();
        }
    }

    private byte[] loadRSShard(DownloadBlockInitResp initresp) throws InterruptedException, ServiceException {
        List<Shard> shards = new ArrayList();
        int len = initresp.getVNF() - UserConfig.Default_PND;
        int nodeindex = 0;
        Map<Integer, Node> map = new HashMap();
        for (Node n : initresp.getNodes()) {
            map.put(n.getId(), n);
        }
        long l = System.currentTimeMillis();
        int retrytimes = 0;
        while (true) {
            int count = len - shards.size();
            if (count <= 0) {
                break;
            }
            if (retrytimes >= 5) {
                throw new ServiceException(SERVER_ERROR);
            }
            if (count > initresp.getNodeids().length - nodeindex) {
                break;
            }
            int sendnum = 0;
            for (int ii = 0; ii < count; ii++) {
                Node n = map.get(initresp.getNodeids()[nodeindex]);
                byte[] VHF = initresp.getVHF()[nodeindex];
                DownloadShardReq req = new DownloadShardReq();
                req.setVHF(VHF);
                nodeindex++;
                if (n == null) {
                    LOG.warn("Node Offline,ID:" + initresp.getNodeids()[nodeindex - 1]);
                    continue;
                }
                DownloadShare.startDownloadShard(VHF, refer.getVBI(), n, this);
                sendnum++;
            }
            synchronized (this) {
                while (resList.size() != sendnum) {
                    this.wait(1000 * 15);
                }
            }
            for (DownloadShardResp res : resList) {
                if (res.getData() != null) {
                    shards.add(new Shard(res.getData()));
                }
            }
            resList.clear();
            retrytimes++;
        }
        if (shards.size() >= len) {
            LOG.info("Download shardcount " + len + ",take time " + (System.currentTimeMillis() - l) + "ms");
            BlockEncrypted be = new BlockEncrypted(refer.getRealSize());
            ShardRSDecoder rsdec = new ShardRSDecoder(shards, be.getEncryptedBlockSize());
            be = rsdec.decode();
            BlockAESDecryptor dec = new BlockAESDecryptor(be.getData(), ks);
            dec.decrypt();
            return dec.getSrcData();
        } else {
            LOG.error("Download shardcount " + shards.size() + ",Not enough shards present.");
            throw new ServiceException(INTERNAL_ERROR);
        }
    }

    private byte[] loadCopyShard(DownloadBlockInitResp initresp) throws ServiceException {
        DownloadShardReq req = new DownloadShardReq();
        int len = initresp.getVNF() * -1;
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
                if (DownloadShare.verify(resp, VHF)) {
                    return aesCopyDecode(resp.getData());
                }
                index++;
            } catch (ServiceException e) {
                t = e;
            }
        }
        LOG.error("Download shardcount " + count + " ERR.");
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
