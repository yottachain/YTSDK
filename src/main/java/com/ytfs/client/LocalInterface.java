package com.ytfs.client;

import com.ytfs.common.ServiceException;
import com.ytfs.common.conf.UserConfig;

public class LocalInterface {

    /**
     * 当前用户统计数据
     *
     * @return json
     * @throws ServiceException
     */
    public static String getUserStat() throws ServiceException {
        /*
        UserSpaceReq req = new UserSpaceReq();
        UserSpaceResp resp = (UserSpaceResp) P2PUtils.requestBPU(req, UserConfig.superNode,6);
        return resp.getJson();
         */
        return null;
    }

    /**
     * 当前用户私钥
     *
     * @return
     */
    public static String getPrivateKey() {
        return UserConfig.username + "@";// + UserConfig.privateKey;
    }

}
