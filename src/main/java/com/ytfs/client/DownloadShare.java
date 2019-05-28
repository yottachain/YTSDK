package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import static com.ytfs.common.conf.UserConfig.DOWNLOADSHARDTHREAD;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.DownloadShardReq;
import com.ytfs.service.packet.DownloadShardResp;
import com.ytfs.common.ServiceException;
import com.ytfs.common.GlobleThreadPool;
import io.yottachain.nodemgmt.core.vo.Node;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

public class DownloadShare implements Runnable {

    private static final Logger LOG = Logger.getLogger(DownloadShare.class);

    private static final ArrayBlockingQueue<DownloadShare> queue;

    static {
        int num = DOWNLOADSHARDTHREAD > 255 ? 255 : DOWNLOADSHARDTHREAD;
        num = num < 5 ? 5 : num;
        queue = new ArrayBlockingQueue(num);
        for (int ii = 0; ii < num; ii++) {
            queue.add(new DownloadShare());
        }
    }

    static void startDownloadShard(byte[] VHF, Node node, DownloadBlock downloadBlock) throws InterruptedException {
        DownloadShare downloader = queue.take();
        downloader.req = new DownloadShardReq();
        downloader.req.setVHF(VHF);
        downloader.downloadBlock = downloadBlock;
        downloader.node = node;
        GlobleThreadPool.execute(downloader);
    }
 
    private DownloadShardReq req;
    private Node node;
    private DownloadBlock downloadBlock;

    static boolean verify(DownloadShardResp resp, byte[] VHF) {
        byte[] data = resp.getData();
        if(data==null){
            LOG.error("VHF Non-existent.");
            return false;
        }
        if (data.length < UserConfig.Default_Shard_Size) {
            return false;
        } else {
            byte[] bs = new byte[UserConfig.Default_Shard_Size];
            System.arraycopy(data, 0, bs, 0, bs.length);
            resp.setData(bs);
        }
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] bs = sha256.digest(resp.getData());
            return Arrays.equals(bs, VHF);
        } catch (NoSuchAlgorithmException ex) {
            return false;
        }
    }

    @Override
    public void run() {
        try {
            DownloadShardResp resp;
            try {
                resp = (DownloadShardResp) P2PUtils.requestNode(req, node);
                if (!verify(resp, req.getVHF())) {
                    LOG.error("VHF inconsistency.");
                    downloadBlock.onResponse(new DownloadShardResp());
                } else {
                    downloadBlock.onResponse(resp);
                }
            } catch (Throwable ex) {                 
                downloadBlock.onResponse(new DownloadShardResp());
            }
        } finally {
            queue.add(this);
        }
    }
}
