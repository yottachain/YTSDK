package com.ytfs.client;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import static com.ytfs.common.conf.UserConfig.superNode;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.RegUserReq;
import com.ytfs.service.packet.RegUserResp;
import io.jafka.jeos.util.KeyUtil;
import io.yottachain.nodemgmt.core.vo.SuperNode;

public class RegUser {

    /**
     * 注册用户
     *
     * @throws ServiceException
     */
    public static void regist() throws ServiceException {
        String prikey = UserConfig.privateKey;
        String ss = KeyUtil.toPublicKey(prikey);
        String pubkey = ss.substring(3);
        RegUserReq req = new RegUserReq();
        req.setPubkey(pubkey);
        req.setUsername(UserConfig.username);
        RegUserResp resp = (RegUserResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        SuperNode sn = new SuperNode(0, null, null, null, null);
        sn.setId(resp.getSuperNodeNum());
        sn.setNodeid(resp.getSuperNodeID());
        sn.setAddrs(resp.getSuperNodeAddrs());
        superNode = sn;
    }

}
