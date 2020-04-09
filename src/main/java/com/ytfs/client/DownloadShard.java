package com.ytfs.client;

import static com.ytfs.client.DownloadShardParam.Max_Retry_Times;
import com.ytfs.common.conf.UserConfig;
import static com.ytfs.common.conf.UserConfig.DOWNLOADSHARDTHREAD;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.DownloadShardReq;
import com.ytfs.service.packet.DownloadShardResp;
import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.codec.lrc.ShardLRCDecoder;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.core.vo.Node;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.log4j.Logger;

public class DownloadShard implements Runnable {

    private static final Logger LOG = Logger.getLogger(DownloadShard.class);

    private static ArrayBlockingQueue<DownloadShard> queue = null;

    public static void init() {
        if (queue == null) {
            queue = new ArrayBlockingQueue(DOWNLOADSHARDTHREAD);
            for (int ii = 0; ii < DOWNLOADSHARDTHREAD; ii++) {
                queue.add(new DownloadShard());
            }
        }
    }

    static void startDownloadShard(ConcurrentLinkedQueue<DownloadShardParam> shardparams, DownloadBlock downloadBlock, ShardLRCDecoder lrcDecoder) throws InterruptedException {
        DownloadShard downloader = queue.take();
        downloader.downloadBlock = downloadBlock;
        downloader.shardparams = shardparams;
        downloader.lrcDecoder = lrcDecoder;
        GlobleThreadPool.execute(downloader);
    }

    private DownloadBlock downloadBlock;
    private ConcurrentLinkedQueue<DownloadShardParam> shardparams;
    private ShardLRCDecoder lrcDecoder;

    @Override
    public void run() {
        try {
            while (true) {
                DownloadShardParam param = shardparams.poll();
                if (param == null) {
                    downloadBlock.onResponse(new DownloadShardResp(), lrcDecoder);
                    break;
                }
                DownloadShardReq req = new DownloadShardReq();
                req.setVHF(param.getVHF());
                Node node = param.getNode();
                try {
                    DownloadShardResp resp = (DownloadShardResp) P2PUtils.requestNode(req, node);
                    if (!verify(resp, req.getVHF(), downloadBlock.refer.getVBI())) {
                        if (lrcDecoder != null) {
                            if (lrcDecoder.isFinished()) {
                                downloadBlock.onResponse(new DownloadShardResp(), lrcDecoder);
                                break;
                            }
                        }
                        LOG.error("[" + downloadBlock.refer.getVBI() + "]Download VHF inconsistency:" + Base58.encode(req.getVHF()) + " from " + node.getId());
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("[" + downloadBlock.refer.getVBI() + "]Download OK:" + Base58.encode(req.getVHF()) + " from " + node.getId());
                        }
                        downloadBlock.onResponse(resp, lrcDecoder);
                        break;
                    }
                } catch (Throwable ex) {
                    if (lrcDecoder != null) {
                        if (lrcDecoder.isFinished()) {
                            downloadBlock.onResponse(new DownloadShardResp(), lrcDecoder);
                            break;
                        }
                    }
                    LOG.error("[" + downloadBlock.refer.getVBI() + "]Download ERR:" + Base58.encode(req.getVHF()) + " from " + node.getId());
                    if (param.getRetryTime() > 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex1) {
                            break;
                        }
                    }
                    if (param.getRetryTime() < Max_Retry_Times) {
                        shardparams.add(param);
                    }
                    param.addRetryTime();
                }
            }
        } finally {
            queue.add(this);
        }
    }

    static boolean verify(DownloadShardResp resp, byte[] VHF, long vbi) {
        byte[] data = resp.getData();
        if (data == null) {
            LOG.error("[" + vbi + "]VHF:(" + Base58.encode(VHF) + ") Non-existent:" + (data == null ? "0" : data.length));
            return false;
        }
        if (data.length < UserConfig.Default_Shard_Size) {
            LOG.error("[" + vbi + "]VHF:(" + Base58.encode(VHF) + ") Invalid data,len=" + data.length);
            return false;
        } else if (data.length == UserConfig.Default_Shard_Size) {
        } else {
            LOG.warn("[" + vbi + "]VHF:(" + Base58.encode(VHF) + ") Data is truncated,len=" + data.length);
            byte[] bs = new byte[UserConfig.Default_Shard_Size];
            System.arraycopy(data, 0, bs, 0, bs.length);
            resp.setData(bs);
        }
        try {
            MessageDigest sha256 = MessageDigest.getInstance("MD5");
            byte[] bs = sha256.digest(resp.getData());
            boolean b = Arrays.equals(bs, VHF);
            if (!b) {
                LOG.error("[" + vbi + "]VHF:(" + Base58.encode(bs) + ")ERR.");
            }
            return b;
        } catch (NoSuchAlgorithmException ex) {
            return false;
        }
    }

}
