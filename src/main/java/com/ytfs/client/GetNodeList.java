package com.ytfs.client;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.user.PreAllocNode;
import com.ytfs.service.packet.user.PreAllocNodeReq;
import com.ytfs.service.packet.user.PreAllocNodeResp;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetNodeList extends Thread {

    static Map<String, PreAllocNodeStat> nodemap = new HashMap();

    private static void getPreAllocNodeResp() throws IOException, ServiceException {
        PreAllocNodeReq req = new PreAllocNodeReq();
        req.setCount(1000);
        PreAllocNodeResp resp = (PreAllocNodeResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
        List<PreAllocNode> ls = resp.getList();
        ls.stream().filter((n) -> !(nodemap.containsKey(n.getNodeid()))).forEachOrdered((n) -> {
            nodemap.put(n.getNodeid(), new PreAllocNodeStat(n, UserConfig.superNode.getId()));
        });
    }
    private static GetNodeList me = null;

    static void init() {
        int ii = 0;
        while (true) {
            try {
                getPreAllocNodeResp();
                ii++;
                if (ii > 5) {
                    break;
                }
            } catch (Throwable r) {
            }
        }
        if (me == null) {
            me = new GetNodeList();
            me.setDaemon(true);
            me.start();
        }
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                getPreAllocNodeResp();
                sleep(60 * 1000);
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
    }
}
