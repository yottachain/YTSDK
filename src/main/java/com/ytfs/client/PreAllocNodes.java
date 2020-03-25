package com.ytfs.client;

import java.util.ArrayList;
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
    
    static final Map<Integer, PreAllocNodeStat> NODE_LIST = new HashMap();
    
    public static void init() {
        PreAllocNodeMgr.init();
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
