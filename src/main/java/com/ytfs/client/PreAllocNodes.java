package com.ytfs.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreAllocNodes {

    static final Map<Integer, PreAllocNodeStat> NODE_LIST = new HashMap();

    public static void init() {
        PreAllocNodeMgr.init();
    }

    public static List<PreAllocNodeStat> getNodes() {
        List<PreAllocNodeStat> ls = new ArrayList(NODE_LIST.values());
        Collections.sort(ls, new PreAllocNodeComparator());
        return ls;
    }

}
