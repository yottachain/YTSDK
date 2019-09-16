package com.ytfs.client;

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
import com.ytfs.service.packet.UploadBlockEndReq;
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

    private ShardRSEncoder rs;
    private final Block block;
    private final short id;
    protected final ObjectId VNU;
    protected final SuperNode bpdNode;
    protected final ConcurrentLinkedQueue<PreAllocNodeStat> excessNode = new ConcurrentLinkedQueue();
    private final Map<Integer, Integer> okTimes = new HashMap();
    private final List<UploadShardRes> resList = new ArrayList();
    private final List<UploadShardRes> okList = new ArrayList();
    private final Map<Integer, Shard> map = new HashMap();
    protected final long sTime;
    protected int retryTimes = 0;
    protected final String signArg;
    protected final long stamp;
    private int maxOkTimes;

    public UploadBlock(Block block, short id, SuperNode bpdNode, ObjectId VNU, long sTime, String signArg, long stamp) {
        this.block = block;
        this.id = id;
        this.bpdNode = bpdNode;
        this.VNU = VNU;
        this.signArg = signArg;
        this.stamp = stamp;
        this.sTime = sTime;
    }

    void onResponse(UploadShardRes res) {
        synchronized (this) {
            resList.add(res);
            if (res.getDNSIGN() != null) {
                okList.add(res);
                Integer ts = okTimes.get(res.getNODEID());
                if (ts == null) {
                    okTimes.put(res.getNODEID(), 1);
                } else {
                    okTimes.put(res.getNODEID(), ts + 1);
                }
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
            long times = firstUpload();
            subUpload(times);
            LOG.info("[" + VNU + "]Upload block " + id + ",shardcount " + len + ",take times " + (System.currentTimeMillis() - l) + "ms");
            completeUploadBlock(ks);
        } catch (Exception r) {
            throw r instanceof ServiceException ? (ServiceException) r : new ServiceException(SERVER_ERROR);
        }
    }

    private long firstUpload() throws InterruptedException {
        List<PreAllocNodeStat> ls = PreAllocNodeMgr.getNodes();
        List<Shard> shards = rs.getShardList();
        if (ls.size() >= shards.size()) {
            if (ls.size() / shards.size() >= 2) {
                this.maxOkTimes = 1;
                this.excessNode.addAll(ls);
            } else {
                this.maxOkTimes = 2;
                this.excessNode.addAll(ls);
                this.excessNode.addAll(ls);
            }
        } else {
            int num = shards.size() / ls.size() + 1;
            maxOkTimes = num * 2;
            for (int ii = 0; ii < num; ii++) {
                this.excessNode.addAll(ls);
            }
        }
        long l = System.currentTimeMillis();
        int shardindex = 0;
        for (Shard sd : shards) {
            map.put(shardindex, sd);
            UploadShard.startUploadShard(this, sd, shardindex);
            shardindex++;
        }
        synchronized (this) {
            while (resList.size() != shards.size()) {
                this.wait(15000);
            }
        }
        return System.currentTimeMillis() - l;
    }

    private void subUpload(long times) throws InterruptedException, ServiceException {
        int retrycount = 0;
        int lasterrnum = 0;
        while (true) {
            List<Integer> shards = new ArrayList();
            resList.stream().filter((res) -> (res.getDNSIGN() == null)).forEach((res) -> {
                shards.add(res.getSHARDID());
            });
            resList.clear();
            int errnum = shards.size();
            if (errnum == 0) {
                return;
            }
            if (errnum != lasterrnum) {
                retrycount = 0;
                lasterrnum = errnum;
            } else {
                retrycount++;
                int interval = (int) times / 12;
                try {
                    Thread.sleep(interval);
                } catch (Exception r) {
                }
            }
            if (retrycount >= UserConfig.RETRYTIMES) {
                LOG.error("[" + VNU + "]Upload block " + id + "," + UserConfig.RETRYTIMES + " retries were unsuccessful.");
                throw new ServiceException(SERVER_ERROR);
            }
            times = secondUpload(shards);
        }
    }

    private long secondUpload(List<Integer> shards) throws InterruptedException {
        List<PreAllocNodeStat> ls = PreAllocNodeMgr.getNodes();
        while (excessNode.size() < shards.size()) {
            ls.forEach((n) -> {
                Integer ok = this.okTimes.get(n.getId());
                if (ok == null || ok < this.maxOkTimes) {
                    excessNode.add(n);
                }
            });
        }
        int errcount = shards.size();
        LOG.info("[" + VNU + "]Upload block " + id + " retrying,remaining " + errcount + " shards.");
        long startTime = System.currentTimeMillis();
        for (Integer shardid : shards) {
            Shard shard = map.get(shardid);
            UploadShard.startUploadShard(this, shard, shardid);
        }
        synchronized (this) {
            while (resList.size() != errcount) {
                this.wait(1000 * 15);
            }
        }
        return System.currentTimeMillis() - startTime;
    }

    private void completeUploadBlock(byte[] ks) throws ServiceException {
        long l = System.currentTimeMillis();
        UploadBlockEndReq req = new UploadBlockEndReq();
        req.setId(id);
        req.setVHP(block.getVHP());
        req.setVHB(rs.makeVHB());
        req.setKEU(KeyStoreCoder.aesEncryped(ks, UserConfig.AESKey));
        req.setKED(KeyStoreCoder.aesEncryped(ks, block.getKD()));
        req.setOriginalSize(block.getOriginalSize());
        req.setRealSize(block.getRealSize());
        req.setRsShard(rs.getShardList().get(0).isRsShard());
        req.setOkList(okList);
        req.setVNU(VNU);
        P2PUtils.requestBPU(req, bpdNode, VNU.toString(), 12);
        LOG.info("[" + VNU + "]Upload block " + id + " OK,take times " + (System.currentTimeMillis() - l) + "ms");
    }
}
