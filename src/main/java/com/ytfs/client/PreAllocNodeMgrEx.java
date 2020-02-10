package com.ytfs.client;

import static com.ytfs.client.PreAllocNodes.idList;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.user.PreAllocNode;
import com.ytfs.service.packet.user.PreAllocNodeReq;
import com.ytfs.service.packet.user.PreAllocNodeResp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class PreAllocNodeMgrEx extends Thread {

    private static final Logger LOG = Logger.getLogger(PreAllocNodeMgrEx.class);
    private static PreAllocNodeMgrEx me = null;


    public static void init() {
        while (true) {
            try {
                PreAllocNodeReq req = new PreAllocNodeReq();
                req.setCount(UserConfig.PNN);
                PreAllocNodeResp resp = (PreAllocNodeResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
                List<PreAllocNode> ls = resp.getList();
                ls.stream().forEach((node) -> {
                    PreAllocNodes.NODE_LIST.put(node.getId(), new PreAllocNodeStat(node));
                });
                LOG.info("Pre-Alloc Node total:" + ls.size());
                me = new PreAllocNodeMgrEx();
                me.start();
                return;
            } catch (Exception ex) {
                LOG.error("Get data node ERR:" + ex);
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException ex1) {
                }
            }
        }
    }

    public static void shutdown() {
        if (me != null) {
            me.interrupt();
        }
    }

    public boolean needAlloc() {
        Collection<PreAllocNodeStat> coll = PreAllocNodes.NODE_LIST.values();
        int count = 0;
        for (PreAllocNodeStat stat : coll) {
            if (idList.contains(stat.getId())) {
                count++;
            }
        }
        if (idList.size() > count) {
            return true;
        } else {
            return false;
        }

    }

    @Override
    public void run() {
        LOG.info("Pre-Alloc Node manager is starting...");
        try {
            if (!needAlloc()) {
                sleep(UserConfig.PTR);
            }
        } catch (InterruptedException ex) {
            this.interrupt();
        }
        while (!this.isInterrupted()) {
            try {
                PreAllocNodeReq req = new PreAllocNodeReq();
                req.setCount(UserConfig.PNN);
                req.setExcludes(ErrorNodeCache.getErrorIds());
                PreAllocNodeResp resp = (PreAllocNodeResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
                updateList(resp.getList());
                if (req.getExcludes().length > 0) {
                    LOG.info("Pre-Alloc Node list is updated,total:" + resp.getList().size() + "," + req.getExcludes().length + " error ids were excluded.");
                } else {
                    LOG.info("Pre-Alloc Node list is updated,total:" + resp.getList().size());
                }
                if (!needAlloc()) {
                    sleep(UserConfig.PTR);
                } else {
                    sleep(UserConfig.PTR / 2);
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

    private void updateList(List<PreAllocNode> ls) {
        int maxsize = PreAllocNodes.NODE_LIST.size();
        Map<Integer, PreAllocNode> map = new HashMap();
        ls.stream().forEach((node) -> {
            map.put(node.getId(), node);
        });
        List<PreAllocNodeStat> removels = new ArrayList();
        List<Map.Entry<Integer, PreAllocNodeStat>> stats = new ArrayList(PreAllocNodes.NODE_LIST.entrySet());
        stats.stream().forEach((ent) -> {
            PreAllocNodeStat stat = ent.getValue();
            if (map.containsKey(ent.getKey())) {
                stat.init(map.remove(ent.getKey()));
                stat.resetStat();
            } else {
                if (idList.contains(ent.getKey())) {
                    stat.resetStat();
                } else {
                    PreAllocNodes.NODE_LIST.remove(ent.getKey());
                    removels.add(ent.getValue());
                }
            }
        });
        Collection<PreAllocNode> coll = map.values();
        coll.stream().forEach((node) -> {
            if (PreAllocNodes.NODE_LIST.size() < maxsize) {
                PreAllocNodes.NODE_LIST.put(node.getId(), new PreAllocNodeStat(node));
            }
        });
        while (PreAllocNodes.NODE_LIST.size() < maxsize && (!removels.isEmpty())) {
            PreAllocNodeStat node = removels.remove(0);
            PreAllocNodes.NODE_LIST.put(node.getId(), node);
        }
    }
}
