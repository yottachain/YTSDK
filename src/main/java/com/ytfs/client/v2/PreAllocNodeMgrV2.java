package com.ytfs.client.v2;

import com.ytfs.client.*;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.user.PreAllocNode;
import com.ytfs.service.packet.user.PreAllocNodeResp;
import com.ytfs.service.packet.v2.PreAllocNodeReqV2;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class PreAllocNodeMgrV2 extends Thread {

    private static final Logger LOG = Logger.getLogger(PreAllocNodeMgrV2.class);
    private static PreAllocNodeMgrV2 me = null;

    private static PreAllocNodeResp getPreAllocNodeResp(YTClient client, int[] errids) throws IOException, ServiceException {
        List<SuperNode> list = new ArrayList(SuperNodeList.getSuperNodeListFromCfg());
        while (true) {
            long index = System.currentTimeMillis() % list.size();
            SuperNode sn = list.remove((int) index);
            try {
                PreAllocNodeReqV2 req = new PreAllocNodeReqV2();
                req.setCount(UserConfig.PNN);
                if (errids != null) {
                    req.setExcludes(errids);
                }
                req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
                PreAllocNodeResp resp = (PreAllocNodeResp) P2PUtils.requestBPU(req, sn, UserConfig.SN_RETRYTIMES);
                return resp;
            } catch (Throwable r) {
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException ex) {
                }
                if (list.isEmpty()) {
                    list = new ArrayList(SuperNodeList.getSuperNodeListFromCfg());
                }
            }
        }
    }

    static synchronized void init(YTClient client) {
        if (me == null) {
            while (true) {
                try {
                    PreAllocNodeResp resp = getPreAllocNodeResp(client, null);
                    List<PreAllocNode> ls = resp.getList();
                    ls.stream().forEach((node) -> {
                        PreAllocNodes.NODE_LIST.put(node.getId(), new PreAllocNodeStat(node));
                    });
                    LOG.info("Pre-Alloc Node total:" + ls.size());
                    me = new PreAllocNodeMgrV2();
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
                List<YTClient> clients = YTClientMgr.getYTClients();
                if (!clients.isEmpty()) {
                    int[] errids = ErrorNodeCache.getErrorIds();
                    PreAllocNodeResp resp = getPreAllocNodeResp(clients.get(0), errids);
                    PreAllocNodes.updateList(resp.getList());
                    if (errids.length > 0) {
                        LOG.info("Pre-Alloc Node list is updated,total:" + resp.getList().size() + "," + errids.length + " error ids were excluded.");
                    } else {
                        LOG.info("Pre-Alloc Node list is updated,total:" + resp.getList().size());
                    }
                }
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

}
