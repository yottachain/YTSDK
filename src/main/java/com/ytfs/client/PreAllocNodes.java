package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.service.packet.user.PreAllocNode;
import io.yottachain.p2phost.YottaP2P;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.tanukisoftware.wrapper.WrapperManager;

public class PreAllocNodes {

    private static final Logger LOG = Logger.getLogger(PreAllocNodes.class);
    static int ALLOC_MODE = 1;
    static int ALLOC_RESET_TIME = 30;

    static {
        String num = WrapperManager.getProperties().getProperty("wrapper.batch.node.allocMode", "1");
        try {
            ALLOC_MODE = Integer.parseInt(num);
        } catch (Exception d) {
            LOG.error("ALLOC_MODE read ERR:" + d.getMessage());
        }
        num = WrapperManager.getProperties().getProperty("wrapper.node.reset.interval", "30");
        try {
            ALLOC_RESET_TIME = Integer.parseInt(num);
        } catch (Exception d) {
            LOG.error("ALLOC_RESET_TIME read ERR:" + d.getMessage());
        }
        LOG.info("ALLOC_MODE:" + ALLOC_MODE);
        if (ALLOC_MODE == 4) {
            GetNodeList.init();
        }
    }

    public static final Map<Integer, PreAllocNodeStat> NODE_LIST = new HashMap();
    private static long lasttime = System.currentTimeMillis();

    private static void resetNODE_LIST() {
        if (System.currentTimeMillis() - lasttime > ALLOC_RESET_TIME * 60 * 1000) {
            NODE_LIST.clear();
            lasttime = System.currentTimeMillis();
        }
    }

    public static void updateList(List<PreAllocNode> ls, int snid) {
        resetNODE_LIST();
        int maxsize = UserConfig.PNN;
        Map<Integer, PreAllocNode> map = new HashMap();
        ls.stream().forEach((node) -> {
            map.put(node.getId(), node);
        });
        List<PreAllocNodeStat> removels = new ArrayList();
        List<Map.Entry<Integer, PreAllocNodeStat>> stats = new ArrayList(PreAllocNodes.NODE_LIST.entrySet());
        stats.stream().forEach((ent) -> {
            PreAllocNodeStat stat = ent.getValue();
            if (map.containsKey(ent.getKey())) {
                stat.init(map.remove(ent.getKey()), snid);
                stat.resetStat();
            } else {
                PreAllocNodes.NODE_LIST.remove(ent.getKey());
                removels.add(ent.getValue());
            }
        });
        Collection<PreAllocNode> coll = map.values();
        coll.stream().forEach((node) -> {
            PreAllocNodes.NODE_LIST.put(node.getId(), new PreAllocNodeStat(node, snid));
        });
        while (PreAllocNodes.NODE_LIST.size() < maxsize && (!removels.isEmpty())) {
            PreAllocNodeStat node = removels.remove(0);
            PreAllocNodes.NODE_LIST.put(node.getId(), node);
        }
    }

    public static List<PreAllocNodeStat> getNodes() {
        if (ALLOC_MODE == 4) {
            long st = System.currentTimeMillis();
            try {
                Set<String> set = GetNodeList.nodemap.keySet();
                List<String> ids = new ArrayList();
                int ii = 0;
                for (String ss : set) {
                    if (ii++ <= 328) {
                        ids.add(ss);
                    }
                }
                List<String> newids = YottaP2P.getOptNodes(ids);
                List<PreAllocNodeStat> ls = new ArrayList();
                LOG.info("Get node priority order OK(" + (System.currentTimeMillis() - st) + " ms)");
                newids.stream().map((nodeid) -> GetNodeList.nodemap.get(nodeid)).filter((s) -> (s != null)).forEachOrdered((s) -> {
                    ls.add(s);
                });
                return ls;
            } catch (Throwable t) {
                LOG.error("Get node priority order ERR(" + (System.currentTimeMillis() - st) + " ms):" + getErrMessage(t));
                List<PreAllocNodeStat> nls = new ArrayList(NODE_LIST.values());
                Collections.sort(nls, new PreAllocNodeComparator());
                return nls;
            }
        }
        if (ALLOC_MODE == 0) {
            Map<String, PreAllocNodeStat> nodemap = new HashMap();
            List<PreAllocNodeStat> ls = new ArrayList(NODE_LIST.values());
            ls.forEach((stat) -> {
                nodemap.put(stat.getNodeid(), stat);
            });
            long st = System.currentTimeMillis();
            try {
                List<String> newids = YottaP2P.getOptNodes(new ArrayList(nodemap.keySet()));
                ls.clear();
                newids.stream().map((nodeid) -> nodemap.get(nodeid)).filter((s) -> (s != null)).forEachOrdered((s) -> {
                    ls.add(s);
                });
                LOG.info("Get node priority order OK(" + (System.currentTimeMillis() - st) + " ms)," + ls.size() + "/" + nodemap.size());
                return ls;
            } catch (Throwable t) {
                LOG.error("Get node priority order ERR(" + (System.currentTimeMillis() - st) + " ms):" + getErrMessage(t));
                List<PreAllocNodeStat> nls = new ArrayList(NODE_LIST.values());
                Collections.sort(nls, new PreAllocNodeComparator());
                return nls;
            }
        }
        List<PreAllocNodeStat> ls = new ArrayList(NODE_LIST.values());
        if (ALLOC_MODE == 1) {
            Collections.sort(ls, new PreAllocNodeComparator());
        } else {
            Collections.shuffle(ls);
        }
        return ls;
    }

    private static String getErrMessage(Throwable err) {
        Throwable t = err;
        while (t != null) {
            if (t.getMessage() == null || t.getMessage().isEmpty()) {
                t = t.getCause();
                continue;
            } else {
                return t.getMessage();
            }
        }
        return "";
    }
}
