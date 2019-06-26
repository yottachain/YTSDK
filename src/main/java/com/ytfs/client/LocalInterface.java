package com.ytfs.client;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.user.UserSpaceReq;
import com.ytfs.service.packet.user.UserSpaceResp;

public class LocalInterface {

    /**
     * 当前用户统计数据
     *
     * @return json
     * @throws ServiceException
     */
    public static String getUserStat() throws ServiceException {
        UserSpaceReq req = new UserSpaceReq();
        UserSpaceResp resp = (UserSpaceResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        return resp.getJson();
    }

    /**
     * 当前用户私钥
     *
     * @return
     */
    public static String getPrivateKey() {
        return UserConfig.username + "@" + UserConfig.privateKey;
    }

}
