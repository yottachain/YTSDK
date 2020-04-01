package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.service.packet.user.PreAllocNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.tanukisoftware.wrapper.WrapperManager;

public class PreAllocNodes {

    static int ALLOC_MODE = 0;

    static {
        String num = WrapperManager.getProperties().getProperty("wrapper.batch.node.allocMode", "0");
        try {
            ALLOC_MODE = Integer.parseInt(num);
        } catch (Exception d) {
        }
    }

    public static final Map<Integer, PreAllocNodeStat> NODE_LIST = new HashMap();

    public static void updateList(List<PreAllocNode> ls,int snid) {
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
                stat.init(map.remove(ent.getKey()),snid);
                stat.resetStat();
            } else {
                PreAllocNodes.NODE_LIST.remove(ent.getKey());
                removels.add(ent.getValue());
            }
        });
        Collection<PreAllocNode> coll = map.values();
        coll.stream().forEach((node) -> {
            PreAllocNodes.NODE_LIST.put(node.getId(), new PreAllocNodeStat(node,snid));
        });
        while (PreAllocNodes.NODE_LIST.size() < maxsize && (!removels.isEmpty())) {
            PreAllocNodeStat node = removels.remove(0);
            PreAllocNodes.NODE_LIST.put(node.getId(), node);
        }
    }

    public static List<PreAllocNodeStat> getNodes() {
        List<PreAllocNodeStat> ls = new ArrayList(NODE_LIST.values());
        if (ALLOC_MODE == 0) {
            Collections.sort(ls, new PreAllocNodeComparator());
        } else {
            Collections.shuffle(ls);
        }
        return ls;
    }

}
