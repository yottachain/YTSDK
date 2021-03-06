package com.ytfs.client;

import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.user.UploadBlockEndReq;
import com.ytfs.service.packet.user.UploadBlockEndResp;
import com.ytfs.service.packet.user.UploadBlockEndSyncReq;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class BlockSyncCache {

    private static final Logger LOG = Logger.getLogger(BlockSyncCache.class);
    private static final Map<UploadBlockEndSyncReq, BlockIP> blockSyncCache = new ConcurrentHashMap<>();
    private static final int ExpiredTime =  60000;
    private static BlockSyncCacheClear me = null;

    public static void putBlock(UploadBlockEndReq req, UploadBlockEndResp resp) {
        synchronized (BlockSyncCache.class) {
            if (me == null) {
                me = new BlockSyncCacheClear();
                me.setDaemon(true);
                me.start();
            }
        }

        UploadBlockEndSyncReq newreq = new UploadBlockEndSyncReq();
        newreq.setAR(req.getAR());
        newreq.setId(req.getId());
        newreq.setKED(req.getKED());
        newreq.setKEU(req.getKEU());
        newreq.setOkList(req.getOkList());
        newreq.setOriginalSize(req.getOriginalSize());
        newreq.setRealSize(req.getRealSize());
        newreq.setVBI(resp.getVBI());
        newreq.setVHB(req.getVHB());
        newreq.setVHP(req.getVHP());
        newreq.setVNU(req.getVNU());
        BlockIP ip = new BlockIP();
        ip.host = resp.getHost();
        ip.time = System.currentTimeMillis();
        blockSyncCache.put(newreq, ip);
    }

    private static boolean checkDomain(UploadBlockEndSyncReq req, BlockIP ip) {
        SuperNode sn = SuperNodeList.getBlockSuperNode(req.getVHP());
        String newip = SuperNodeList.getSelfIp(sn.getId());
        if (ip.host.equalsIgnoreCase(newip)) {
            return true;
        }
        try {
            P2PUtils.requestBPU(req, sn, req.getVNU().toString(), 0);
            LOG.info("[" + req.getVNU() + "][" + req.getId() + "]Upload block OK.");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static class BlockIP {

        private String host;
        private long time;
    }

    private static class BlockSyncCacheClear extends Thread {

        @Override
        public void run() {
            while (!this.isInterrupted()) {
                try {
                    sleep(1000 * 15);
                    List<Map.Entry<UploadBlockEndSyncReq, BlockIP>> set = new ArrayList(blockSyncCache.entrySet());
                    for (Map.Entry<UploadBlockEndSyncReq, BlockIP> ent : set) {
                        if (System.currentTimeMillis() - ent.getValue().time > ExpiredTime) {
                            if (checkDomain(ent.getKey(), ent.getValue())) {
                                blockSyncCache.remove(ent.getKey());
                            }
                        }
                    }
                    sleep(1000 * 15);
                } catch (Throwable e) {
                    LOG.info("ERR:" + e.getMessage());
                }
            }
        }
    }
}
