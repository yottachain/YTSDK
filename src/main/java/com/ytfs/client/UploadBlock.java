package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.common.codec.Block;
import com.ytfs.common.codec.BlockAESEncryptor;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.codec.Shard;
import com.ytfs.common.codec.erasure.ShardRSEncoder;
import com.ytfs.common.net.P2PUtils;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.ShardEncoder;
import com.ytfs.common.codec.lrc.ShardLRCEncoder;
import static com.ytfs.common.conf.UserConfig.SN_RETRYTIMES;
import com.ytfs.service.packet.user.UploadBlockEndReq;
import com.ytfs.service.packet.user.UploadBlockEndResp;
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

    protected final short id;
    protected final ObjectId VNU;
    protected final SuperNode bpdNode;
    protected final ConcurrentLinkedQueue<PreAllocNodeStat> excessNode = new ConcurrentLinkedQueue();
    protected final long sTime;
    protected int retryTimes = 0;
    protected final String signArg;
    protected final long stamp;
    private int maxOkTimes;
    private ShardEncoder encoder;
    private final Block block;
    private final Map<Integer, Integer> okTimes = new HashMap();
    private final List<UploadShardRes> resList = new ArrayList();
    private final List<UploadShardRes> okList = new ArrayList();
    private final Map<Integer, Shard> map = new HashMap();

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
            if (UserConfig.useLRCCoder) {
                encoder = new ShardLRCEncoder(aes.getBlockEncrypted());
            } else {
                encoder = new ShardRSEncoder(aes.getBlockEncrypted());
            }
            encoder.encode();
            /*
            if (encoder.getShardList().size() == 164) {
                int ii = 0;
                MessageWriter.write("d:\\src.dat", aes.getBlockEncrypted().getData());
                for (Shard s : encoder.getShardList()) {
                    MessageWriter.write("d:\\" + ii + ".dat", s.getData());
                    ii++;
                }
            }
             */
            long times = firstUpload();
            subUpload(times);
            LOG.info("[" + VNU + "][" + id + "]Upload block OK,shardcount " + encoder.getShardList().size() + ",take times " + (System.currentTimeMillis() - l) + "ms");
            completeUploadBlock(ks);
        } catch (Exception r) {
            throw r instanceof ServiceException ? (ServiceException) r : new ServiceException(SERVER_ERROR);
        }
    }

    private long firstUpload() throws InterruptedException {
        List<PreAllocNodeStat> ls = PreAllocNodeMgr.getNodes();
        List<Shard> shards = encoder.getShardList();
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
                    Thread.sleep(interval < 1000 ? 1000 : interval);
                } catch (Exception r) {
                }
            }
            if (retrycount >= UserConfig.RETRYTIMES) {
                LOG.error("[" + VNU + "][" + id + "]Upload block " + UserConfig.RETRYTIMES + " retries were unsuccessful.");
                throw new ServiceException(SERVER_ERROR);
            }
            fillExcessNode(shards);
            times = secondUpload(shards);
        }
    }

    private void fillExcessNode(List<Integer> shards) throws ServiceException {
        int preAllocNodeTimes = 0;
        while (true) {
            List<PreAllocNodeStat> ls = PreAllocNodeMgr.getNodes();
            ls.forEach((n) -> {
                Integer ok = this.okTimes.get(n.getId());
                if (ok == null || ok < this.maxOkTimes) {
                    excessNode.add(n);
                }
            });
            if (excessNode.size() < shards.size()) {
                if (preAllocNodeTimes > 2) {
                    LOG.error("[" + VNU + "][" + id + "]Not enough nodes to upload shards,upload aborted.");
                    throw new ServiceException(SERVER_ERROR);
                } else {
                    LOG.error("[" + VNU + "][" + id + "]Not enough nodes to upload shards,waiting...");
                    preAllocNodeTimes++;
                    try {
                        Thread.sleep(UserConfig.PTR);
                    } catch (Exception r) {
                    }
                }
            } else {
                break;
            }
        }
    }

    private long secondUpload(List<Integer> shards) throws InterruptedException {
        int errcount = shards.size();
        LOG.info("[" + VNU + "][" + id + "]Upload block is still incomplete,remaining " + errcount + " shards.");
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

    /**
     * 确认块
     *
     * @param ks
     * @throws ServiceException
     */
    private void completeUploadBlock(byte[] ks) throws ServiceException {
        long l = System.currentTimeMillis();
        UploadBlockEndReq req = new UploadBlockEndReq();
        req.setId(id);
        req.setVHP(block.getVHP());
        req.setVHB(encoder.makeVHB());
        req.setKEU(KeyStoreCoder.aesEncryped(ks, UserConfig.AESKey));
        req.setKED(KeyStoreCoder.aesEncryped(ks, block.getKD()));
        req.setOriginalSize(block.getOriginalSize());
        req.setRealSize(block.getRealSize());
        if (encoder.isCopyMode()) {
            req.setAR(ShardEncoder.AR_COPY_MODE);
        } else {
            if (encoder instanceof ShardRSEncoder) {
                req.setAR(ShardEncoder.AR_RS_MODE);
            } else {
                req.setAR(encoder.getDataCount());
            }
        }
        req.setOkList(okList);
        req.setVNU(VNU);
        Object obj = P2PUtils.requestBPU(req, bpdNode, VNU.toString(), SN_RETRYTIMES);//重试5分钟
        if (obj instanceof UploadBlockEndResp) {
            BlockSyncCache.putBlock(req, (UploadBlockEndResp) obj);
        }
        LOG.info("[" + VNU + "][" + id + "]Upload block OK,take times " + (System.currentTimeMillis() - l) + "ms");
    }
}
