package com.ytfs.client;

import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.eos.EOSRequest;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.user.PreRegUserReq;
import com.ytfs.service.packet.user.PreRegUserResp;
import com.ytfs.service.packet.user.RegUserReq;
import com.ytfs.service.packet.user.RegUserResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import org.apache.log4j.Logger;

public class RegUser {

    private static final Logger LOG = Logger.getLogger(RegUser.class);

    /**
     * 注册用户
     *
     * @param sNode
     * @throws ServiceException
     */
    public static void regist(SuperNode sNode) throws ServiceException {
        try {
            PreRegUserReq preq = new PreRegUserReq();
            PreRegUserResp presp = (PreRegUserResp) P2PUtils.requestBPU(preq, sNode);
            RegUserReq req = new RegUserReq();
            byte[] signData = EOSRequest.makeGetBalanceRequest(presp.getSignArg(), UserConfig.username,
                    UserConfig.privateKey, presp.getContractAccount());
            req.setSigndata(signData);
            req.setUsername(UserConfig.username);
            RegUserResp resp = (RegUserResp) P2PUtils.requestBPU(req, sNode);
            SuperNode sn = new SuperNode(0, null, null, null, null);
            sn.setId(resp.getSuperNodeNum());
            sn.setNodeid(resp.getSuperNodeID());
            sn.setAddrs(resp.getSuperNodeAddrs());
            LOG.info("Current user supernode:" + sn.getId() + ",ID:" + sn.getNodeid());
            UserConfig.superNode = sn;
        } catch (Exception r) {
            throw new ServiceException(SERVER_ERROR);
        }
    }

}
