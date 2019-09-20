package com.ytfs.client;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.user.PreAllocNode;
import com.ytfs.service.packet.user.PreAllocNodeReq;
import com.ytfs.service.packet.user.PreAllocNodeResp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class PreAllocNodeMgr extends Thread {

    private static final Logger LOG = Logger.getLogger(PreAllocNodeMgr.class);
    private static PreAllocNodeMgr me = null;

    private static final Map<Integer, PreAllocNodeStat> NODE_LIST = new HashMap();

    public static void init() {
        Exception err = null;
        for (int ii = 0; ii < 10; ii++) {
            try {
                PreAllocNodeReq req = new PreAllocNodeReq();
                req.setCount(UserConfig.PNN);
                PreAllocNodeResp resp = (PreAllocNodeResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
                List<PreAllocNode> ls = resp.getList();
                ls.stream().forEach((node) -> {
                    NODE_LIST.put(node.getId(), new PreAllocNodeStat(node));
                });
                me = new PreAllocNodeMgr();
                me.start();
                return;
            } catch (ServiceException ex) {
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException ex1) {
                }
                err = ex;
            }
        }
        LOG.error("Get data node ERR:" + err);
    }

    public static List<PreAllocNodeStat> getNodes() {
        List<PreAllocNodeStat> ls = new ArrayList(NODE_LIST.values());
        Collections.sort(ls, new PreAllocNodeComparator());
        return ls;
    }

    public static void shutdown() {
        if (me != null) {
            me.interrupt();
        }
    }

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
                PreAllocNodeReq req = new PreAllocNodeReq();
                req.setCount(UserConfig.PNN);
                PreAllocNodeResp resp = (PreAllocNodeResp) P2PUtils.requestBPU(req, UserConfig.superNode, 6);
                updateList(resp.getList());
                LOG.info("Pre-Alloc Node list is updated.");
                sleep(UserConfig.PTR);
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

    private void updateList(List<PreAllocNode> ls) {
        Map<Integer, PreAllocNode> map = new HashMap();
        ls.stream().forEach((node) -> {
            map.put(node.getId(), node);
        });
        List<Map.Entry<Integer, PreAllocNodeStat>> stats = new ArrayList(NODE_LIST.entrySet());
        stats.stream().forEach((ent) -> {
            PreAllocNodeStat stat = ent.getValue();
            if (map.containsKey(ent.getKey())) {
                stat.init(map.remove(ent.getKey()));
                stat.resetStat();
            } else {
                NODE_LIST.remove(ent.getKey());
                stat.disconnet();
            }
        });
        Collection<PreAllocNode> coll = map.values();
        coll.stream().forEach((node) -> {
            NODE_LIST.put(node.getId(), new PreAllocNodeStat(node));
        });
    }
}
