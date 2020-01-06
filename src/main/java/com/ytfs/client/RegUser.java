package com.ytfs.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.eos.EOSRequest;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.user.PreRegUserReq;
import com.ytfs.service.packet.user.PreRegUserResp;
import com.ytfs.service.packet.user.RegUserReq;
import com.ytfs.service.packet.user.RegUserResp;
import io.jafka.jeos.util.KeyUtil;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class RegUser {

    private static final Logger LOG = Logger.getLogger(RegUser.class);

    public static void regist() throws IOException {
        String path = System.getProperty("snlist.conf", "conf/snlist.properties");
        InputStream is = null;
        try {
            is = new FileInputStream(path);
        } catch (Exception r) {
        }
        if (is == null) {
            throw new IOException("No snlist properties file could be found for ytfs service");
        }
        List snlist;
        try {
            ObjectMapper mapper = new ObjectMapper();
            snlist = mapper.readValue(is, ArrayList.class);
        } finally {
            is.close();
        }
        if (snlist == null || snlist.isEmpty()) {
            throw new IOException("No snlist properties file could be found for ytfs service");
        }
        List list = new ArrayList(snlist);
        while (true) {
            long index = System.currentTimeMillis() % list.size();
            Map map = (Map) list.remove((int) index);
            try {
                SuperNode sn = new SuperNode(0, null, null, null, null);
                sn.setId(Integer.parseInt(map.get("Number").toString()));
                sn.setNodeid(map.get("ID").toString());
                List addr = (List) map.get("Addrs");
                sn.setAddrs(addr);
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
                    list = new ArrayList(snlist);
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
            PreRegUserReq preq = new PreRegUserReq();
            PreRegUserResp presp = (PreRegUserResp) P2PUtils.requestBPU(preq, sNode, 0);
            RegUserReq req = new RegUserReq();
            byte[] signData = EOSRequest.makeGetBalanceRequest(presp.getSignArg(), UserConfig.username,
                    UserConfig.privateKey, presp.getContractAccount());
            req.setSigndata(signData);
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
            r.printStackTrace();
            throw new ServiceException(SERVER_ERROR);
        }
    }

}
