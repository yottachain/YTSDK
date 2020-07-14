package com.ytfs.client;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.user.PreAllocNode;
import com.ytfs.service.packet.user.PreAllocNodeReq;
import com.ytfs.service.packet.user.PreAllocNodeResp;
import java.io.IOException;
import java.util.List;
import org.apache.log4j.Logger;

public class PreAllocNodeMgr extends Thread {

    private static final Logger LOG = Logger.getLogger(PreAllocNodeMgr.class);
    private static PreAllocNodeMgr me = null;

    private static PreAllocNodeResp getPreAllocNodeResp(int[] errids) throws IOException, ServiceException {
        PreAllocNodeReq req = new PreAllocNodeReq();
        req.setCount(UserConfig.PNN);
        if (errids != null) {
            req.setExcludes(ErrorNodeCache.getErrorIds());
        }
        PreAllocNodeResp resp = (PreAllocNodeResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
        return resp;
    }

    public static void Reset() {
        if (me != null) {
            synchronized (me) {
                me.direct=true;
                me.notify();
            }
        }
    }

    static synchronized void init() {
        if (me == null) {
            while (true) {
                try {
                    PreAllocNodeResp resp = getPreAllocNodeResp(null);
                    List<PreAllocNode> ls = resp.getList();
                    ls.stream().forEach((node) -> {
                        PreAllocNodes.NODE_LIST.put(node.getId(), new PreAllocNodeStat(node, UserConfig.superNode.getId()));
                    });
                    LOG.info("Pre-Alloc Node total:" + ls.size());
                    me = new PreAllocNodeMgr();
                    me.start();
                    return;
                } catch (Throwable ex) {
                    LOG.error("Get data node ERR:" + ex);
                    try {
                        Thread.sleep(15000);
                    } catch (InterruptedException ex1) {
                    }
                }
            }
        }
    }

    public static void shutdown() {
        if (me != null) {
            me.interrupt();
            me = null;
        }
    }

    boolean direct=false;
    @Override
    public void run() {
        LOG.info("Pre-Alloc Node manager is starting...");
        try {
            sleep(UserConfig.PTR);
        } catch (InterruptedException ex) {
            this.interrupt();
        }
        while (!this.isInterrupted()) {
            try {
                int[] errids = ErrorNodeCache.getErrorIds();
                PreAllocNodeResp resp = getPreAllocNodeResp(errids);
                PreAllocNodes.updateList(resp.getList(), UserConfig.superNode.getId(),direct);
                if (errids.length > 0) {
                    LOG.info("Pre-Alloc Node list is updated,total:" + resp.getList().size() + "," + errids.length + " error ids were excluded.");
                } else {
                    LOG.info("Pre-Alloc Node list is updated,total:" + resp.getList().size());
                }
                synchronized (this) {
                    direct=false;
                    this.wait(UserConfig.PTR);
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Throwable ex) {
                try {
                    sleep(15000);
                } catch (InterruptedException ex1) {
                    break;
                }
            }
        }
        LOG.info("Pre-Alloc Node manager is safely closed.");
    }
}
