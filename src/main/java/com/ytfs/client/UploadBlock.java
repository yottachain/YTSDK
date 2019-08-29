package com.ytfs.client;

import com.ytfs.common.Function;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.common.codec.Block;
import com.ytfs.common.codec.BlockAESEncryptor;
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
import com.ytfs.service.packet.bp.ActiveCache;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final List<UploadShardRes> okList = new ArrayList();
    private final Map<Integer, Shard> map = new HashMap();
    protected final long sTime;
    protected int retryTimes = 0;

    public UploadBlock(Block block, short id, ShardNode[] nodes, ShardNode[] excessNodes, long VBI, SuperNode bpdNode, ObjectId VNU) {
        this.block = block;
        this.id = id;
        this.nodes = nodes;
        this.excessNode.addAll(Arrays.asList(excessNodes));
        this.VBI = VBI;
        this.bpdNode = bpdNode;
        this.VNU = VNU;
        this.sTime = getStartTime(VBI);
    }

    private long getStartTime(long vbi) {
        long curTime = System.currentTimeMillis();
        byte[] bs = Function.long2bytes(vbi);
        long time = Function.bytes2Integer(bs, 0, 4);
        time = time * 1000 + (curTime % 1000);
        return time;
    }

    void onResponse(UploadShardRes res) {
        synchronized (this) {
            resList.add(res);
            if (res.getDNSIGN() != null) {
                okList.add(res);
            }
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
        req.setOkList(okList);
        req.setVNU(VNU);
        P2PUtils.requestBPU(req, bpdNode, VNU.toString());
        LOG.info("[" + VNU + "]Upload block " + id + "/" + VBI + " OK,take times " + (System.currentTimeMillis() - l) + "ms");
    }

    private void firstUpload() throws InterruptedException {
        List<Shard> shards = rs.getShardList();
        int nodeindex = 0;
        long startTime = System.currentTimeMillis();
        for (Shard sd : shards) {
            map.put(nodeindex, sd);
            while (true) {
                boolean b = UploadShard.startUploadShard(this, nodes[nodeindex], sd, nodeindex);
                if (System.currentTimeMillis() - startTime >= 60000) {
                    sendActive();
                    startTime = System.currentTimeMillis();
                }
                if (b) {
                    break;
                }
            }
            nodeindex++;
        }
        long times = 0;
        synchronized (this) {
            while (resList.size() != shards.size()) {
                this.wait(15000);
                times = times + 15000;
                if (times >= 60000) {
                    sendActive();
                    times = 0;
                }
            }
        }
    }

    private void sendActive() {
        try {
            ActiveCache active = new ActiveCache();
            active.setVNU(VNU);
            P2PUtils.requestBPU(active, bpdNode, VNU.toString());
        } catch (Exception r) {
        }
    }

    private void subUpload() throws InterruptedException, ServiceException {
        int retrycount = 0;
        int lasterrnum = 0;
        while (true) {
            List<Integer> shards = new ArrayList();
            List<Integer> errid = new ArrayList();
            resList.stream().filter((res) -> (res.getDNSIGN() == null)).forEach((res) -> {
                errid.add(res.getNODEID());
                shards.add(res.getSHARDID());
            });
            resList.clear();
            UploadBlockSubReq uloadBlockSubReq = doUploadShardRes(errid);
            if (uloadBlockSubReq == null) {
                return;
            } else {
                int errnum = uloadBlockSubReq.getShardCount();
                if (errnum != lasterrnum) {
                    retrycount = 0;
                    lasterrnum = errnum;
                } else {
                    retrycount++;
                    try {
                        Thread.sleep(interval);
                    } catch (Exception r) {
                    }
                }
                if (retrycount >= retrytimes) {
                    LOG.error("[" + VNU + "]Upload block " + id + "/" + VBI + " " + retrytimes + " retries were unsuccessful.");
                    throw new ServiceException(SERVER_ERROR);
                }
            }
            retryTimes++;
            UploadBlockSubResp resp = (UploadBlockSubResp) P2PUtils.requestBPU(uloadBlockSubReq, bpdNode, VNU.toString());
            if (resp.getNodes() == null || resp.getNodes().length == 0) {//OK
                break;
            }
            secondUpload(resp, shards);
        }
    }

    private void secondUpload(UploadBlockSubResp resp, List<Integer> shards) throws InterruptedException {
        excessNode.clear();
        ShardNode[] respNodes = resp.getNodes();
        excessNode.addAll(Arrays.asList(resp.getExcessNodes()));
        int errcount = shards.size();
        LOG.info("[" + VNU + "]Upload block " + id + "/" + VBI + " retrying,remaining " + errcount + " shards.");
        long startTime = System.currentTimeMillis();
        int nodeindex = 0;
        for (Integer shardid : shards) {
            ShardNode node = respNodes[nodeindex];
            Shard shard = map.get(shardid);
            while (true) {
                boolean b = UploadShard.startUploadShard(this, node, shard, shardid);
                if (System.currentTimeMillis() - startTime >= 60000) {
                    sendActive();
                    startTime = System.currentTimeMillis();
                }
                if (b) {
                    break;
                }
            }
            nodeindex++;
        }
        long times = 0;
        synchronized (this) {
            while (resList.size() != errcount) {
                this.wait(1000 * 15);
                times = times + 15000;
                if (times >= 60000) {
                    sendActive();
                    times = 0;
                }
            }
        }
    }

    private UploadBlockSubReq doUploadShardRes(List<Integer> errid) {
        if (errid.isEmpty()) {
            return null;
        } else {
            UploadBlockSubReq subreq = new UploadBlockSubReq();
            subreq.setVNU(VNU);
            subreq.setErrid(errid);
            subreq.setVBI(VBI);
            subreq.setShardCount(errid.size());
            return subreq;
        }
    }
}
