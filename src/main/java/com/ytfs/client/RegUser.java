package com.ytfs.client;

import com.ytfs.client.v2.YTClient;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.user.RegUserReq;
import com.ytfs.service.packet.user.RegUserResp;
import com.ytfs.service.packet.v2.RegUserReqV2;
import io.jafka.jeos.util.KeyUtil;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class RegUser {

    private static final Logger LOG = Logger.getLogger(RegUser.class);

    public static void regist(YTClient client) throws IOException {
        List<SuperNode> list = new ArrayList(SuperNodeList.getSuperNodeListFromCfg());
        long index = System.currentTimeMillis() % list.size();
        SuperNode sn = list.remove((int) index);
        try {
            RegUser.regist(sn, client);
            LOG.info("User '" + client.getUsername() + "' Registration Successful,ID:"+client.getUserId());
        } catch (Throwable r) {
            LOG.info("User '" + client.getUsername()+ "' registration failed:" + r.getMessage());
            throw r instanceof IOException?(IOException) r:new IOException(r);
        }
    }

    /**
     * 注册用户
     *
     * @param sNode
     * @throws ServiceException
     */
    private static void regist(SuperNode sNode, YTClient client) throws ServiceException {
        try {
            RegUserReqV2 req = new RegUserReqV2();
            req.setUsername(client.getUsername());
            String pubkey = KeyUtil.toPublicKey(client.getPrivateKey());
            req.setPubKey(pubkey.substring(3));
            SuperNode sn = new SuperNode(0, null, null, null, null);
            RegUserResp resp = (RegUserResp) P2PUtils.requestBPU(req, sNode, 0);
            sn.setId(resp.getSuperNodeNum());
            sn.setNodeid(resp.getSuperNodeID());
            sn.setAddrs(resp.getSuperNodeAddrs());
            client.setUserId(resp.getUserId());
            client.setKeyNumber(resp.getKeyNumber());
            client.setSuperNode(sn);
            LOG.info("supernode:" + sn.getId() + ",ID:" + sn.getNodeid());
        } catch (Exception r) {
            throw new ServiceException(SERVER_ERROR);
        }
    }

    public static void regist() throws IOException {
        List<SuperNode> list = new ArrayList(SuperNodeList.getSuperNodeListFromCfg());
        while (true) {
            long index = System.currentTimeMillis() % list.size();
            SuperNode sn = list.remove((int) index);
            try {
                RegUser.regist(sn);
                LOG.info("User Registration Successful.");
                return;
            } catch (Throwable r) {
                LOG.info("User registration failed:" + r.getMessage());
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

    /**
     * 注册用户
     *
     * @param sNode
     * @throws ServiceException
     */
    private static void regist(SuperNode sNode) throws ServiceException {
        try {
            RegUserReq req = new RegUserReq();
            req.setUsername(UserConfig.username);
            String pubkey = KeyUtil.toPublicKey(UserConfig.privateKey);
            req.setPubKey(pubkey.substring(3));
            RegUserResp resp = (RegUserResp) P2PUtils.requestBPU(req, sNode, 0);
            SuperNode sn = new SuperNode(0, null, null, null, null);
            sn.setId(resp.getSuperNodeNum());
            sn.setNodeid(resp.getSuperNodeID());
            sn.setAddrs(resp.getSuperNodeAddrs());
            UserConfig.userId = resp.getUserId();
            UserConfig.keyNumber = resp.getKeyNumber();
            LOG.info("Current user ID:" + resp.getUserId() + ",supernode:" + sn.getId() + ",ID:" + sn.getNodeid());
            UserConfig.superNode = sn;
        } catch (Exception r) {
            throw new ServiceException(SERVER_ERROR);
        }
    }

}
