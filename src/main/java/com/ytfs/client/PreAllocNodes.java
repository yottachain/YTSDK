package com.ytfs.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class PreAllocNodes {

    private static final Logger LOG = Logger.getLogger(PreAllocNodes.class);

    static List<Integer> idList = null;
    static final Map<Integer, PreAllocNodeStat> NODE_LIST = new HashMap();

    public static void init() {
        try {
            loadIds();
        } catch (IOException ex) {
        }
        if (idList == null) {
            PreAllocNodeMgr.init();
        } else {
            PreAllocNodeMgrEx.init();
        }
    }

    public static List<PreAllocNodeStat> getNodes() {
        List<PreAllocNodeStat> ls = new ArrayList(NODE_LIST.values());
        Collections.sort(ls, new PreAllocNodeComparator());
        return ls;
    }

    private static void loadIds() throws IOException {
        String path = System.getProperty("idlist.conf");
        File file;
        if (path == null) {
            path = "conf/idlist.properties";
            file = new File(path);
        } else {
            file = new File(path);
            if (!file.exists()) {
                file = new File("conf/idlist.properties");
            }
        }
        if (!file.exists()) {
            file = new File("../conf/idlist.properties");
        }
        InputStream is = null;
        try {
            is = new FileInputStream(file);
        } catch (FileNotFoundException r) {
            return;
        }
        List idlist;
        try {
            ObjectMapper mapper = new ObjectMapper();
            idlist = mapper.readValue(is, ArrayList.class);
        } finally {
            is.close();
        }
        if (idlist != null && !idlist.isEmpty()) {
            idList = idlist;
            LOG.warn("Idlist properties file is loaded:" + idlist.size());
        }
    }
}
