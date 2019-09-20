package com.ytfs.client;

import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.Block;
import com.ytfs.common.codec.BlockAESEncryptor;
import com.ytfs.common.codec.BlockEncrypted;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.codec.ShardRSEncoder;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.eos.EOSRequest;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.UploadBlockDBReq;
import com.ytfs.service.packet.UploadBlockDupReq;
import com.ytfs.service.packet.UploadBlockDupResp;
import com.ytfs.service.packet.UploadBlockInitReq;
import com.ytfs.service.packet.UploadBlockInitResp;
import com.ytfs.service.packet.UploadObjectEndReq;
import com.ytfs.service.packet.user.PreSubBalanceReq;
import com.ytfs.service.packet.user.PreSubBalanceResp;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import java.util.Arrays;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public abstract class UploadObjectAbstract {

    private static final Logger LOG = Logger.getLogger(UploadObjectSlow.class);

    protected ObjectId VNU;
    protected byte[] VHW;
    protected String signArg;
    protected long stamp;

    public abstract byte[] upload() throws ServiceException, IOException, InterruptedException;

    public final byte[] getVHW() {
        return VHW;
    }

    //结束上传
    protected final void complete() throws ServiceException {
        ServiceException err = null;
        int times = 0;
        while (true) {
            try {
                PreSubBalanceReq subreq = new PreSubBalanceReq();
                subreq.setVNU(VNU);
                PreSubBalanceResp resp = (PreSubBalanceResp) P2PUtils.requestBPU(subreq, UserConfig.superNode, VNU.toString(), 0);
                UploadObjectEndReq req = new UploadObjectEndReq();
                req.setVHW(VHW);
                req.setVNU(VNU);
                try {
                    byte[] bs = resp.getSignArg();
                    byte[] signData = EOSRequest.makeSubBalanceRequest(bs, UserConfig.username,
                            UserConfig.privateKey, resp.getContractAccount(), resp.getFirstCost(), resp.getUserid());
                    req.setSignData(signData);
                } catch (Throwable e) {
                    err = new ServiceException(SERVER_ERROR, e.getMessage());
                    break;
                }
                P2PUtils.requestBPU(req, UserConfig.superNode, VNU.toString(), 0);
                return;
            } catch (Throwable ex) {
                err = ex instanceof ServiceException ? (ServiceException) ex : new ServiceException(SERVER_ERROR, ex.getMessage());
                if (times >= UserConfig.SN_RETRYTIMES) {
                    break;
                }
                times++;
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ex1) {
                }
            }
        }
        if (err != null) {
            throw err;
        }
    }
    //上传块

    public final void upload(Block b, short id, SuperNode node) throws ServiceException, InterruptedException {
        long l = System.currentTimeMillis();
        BlockEncrypted be = new BlockEncrypted(b.getRealSize());
        UploadBlockInitReq req = new UploadBlockInitReq(VNU, b.getVHP(), be.getShardCount(), id);
        Object resp = P2PUtils.requestBPU(req, node, VNU.toString(), UserConfig.SN_RETRYTIMES);
        if (resp instanceof UploadBlockDupResp) {//重复,resp.getExist()=0已经上传     
            UploadBlockDupReq uploadBlockDupReq = checkResp((UploadBlockDupResp) resp, b);
            if (uploadBlockDupReq != null) {//请求节点
                uploadBlockDupReq.setId(id);
                uploadBlockDupReq.setVHP(b.getVHP());  //计数
                uploadBlockDupReq.setOriginalSize(b.getOriginalSize());
                uploadBlockDupReq.setRealSize(b.getRealSize());
                uploadBlockDupReq.setVNU(VNU);
                P2PUtils.requestBPU(uploadBlockDupReq, node, VNU.toString(), UserConfig.SN_RETRYTIMES);
                LOG.info("[" + VNU + "]Block " + id + " is a repetitive block:" + Base58.encode(b.getVHP()));
            } else {
                if (!be.needEncode()) {
                    UploadBlockToDB(b, id, node);
                } else {//请求分配节点
                    UploadBlock ub = new UploadBlock(b, id, node, VNU, ((UploadBlockDupResp) resp).getStartTime(), signArg, stamp);
                    LOG.info("[" + VNU + "]Block " + id + " is initialized at sn " + node.getId() + ",take times " + (System.currentTimeMillis() - l) + "ms");
                    ub.upload();
                }
            }
        }
        if (resp instanceof UploadBlockInitResp) {
            if (!be.needEncode()) {
                UploadBlockToDB(b, id, node);
            } else {
                UploadBlock ub = new UploadBlock(b, id, node, VNU, ((UploadBlockInitResp) resp).getStartTime(), signArg, stamp);
                LOG.info("[" + VNU + "]Block " + id + " is initialized at sn " + node.getId() + ",take times " + (System.currentTimeMillis() - l) + "ms");
                ub.upload();
            }
        }
    }

    //上传小文件至数据库
    private void UploadBlockToDB(Block b, short id, SuperNode node) throws ServiceException {
        try {
            byte[] ks = KeyStoreCoder.generateRandomKey();
            BlockAESEncryptor enc = new BlockAESEncryptor(b, ks);
            enc.encrypt();
            UploadBlockDBReq req = new UploadBlockDBReq();
            req.setId(id);
            req.setVNU(VNU);
            req.setVHP(b.getVHP());
            req.setVHB(enc.getBlockEncrypted().getVHB());
            req.setKEU(KeyStoreCoder.aesEncryped(ks, UserConfig.AESKey));
            req.setKED(KeyStoreCoder.aesEncryped(ks, b.getKD()));
            req.setOriginalSize(b.getOriginalSize());
            req.setData(enc.getBlockEncrypted().getData());
            P2PUtils.requestBPU(req, node, VNU.toString(), UserConfig.SN_RETRYTIMES);
            LOG.info("[" + VNU + "]Upload block " + id + " to DB,VHP:" + Base58.encode(b.getVHP()));
        } catch (Exception e) {
            LOG.error("[" + VNU + "]" + e.getMessage());
            throw new ServiceException(SERVER_ERROR);
        }
    }

    //检查重复
    private UploadBlockDupReq checkResp(UploadBlockDupResp resp, Block b) {
        byte[][] keds = resp.getKED();
        byte[][] vhbs = resp.getVHB();
        for (int ii = 0; ii < keds.length; ii++) {
            byte[] ked = keds[ii];
            try {
                byte[] ks = KeyStoreCoder.aesDecryped(ked, b.getKD());
                byte[] VHB;
                BlockAESEncryptor aes = new BlockAESEncryptor(b, ks);
                aes.encrypt();
                if (aes.getBlockEncrypted().needEncode()) {
                    ShardRSEncoder enc = new ShardRSEncoder(aes.getBlockEncrypted());
                    enc.encode();
                    VHB = enc.makeVHB();
                } else {
                    VHB = aes.getBlockEncrypted().getVHB();
                }
                if (Arrays.equals(vhbs[ii], VHB)) {
                    UploadBlockDupReq req = new UploadBlockDupReq();
                    req.setVHB(VHB);
                    byte[] keu = KeyStoreCoder.aesEncryped(ks, UserConfig.AESKey);
                    req.setKEU(keu);
                    return req;
                }
            } catch (Exception r) {//解密不了,认为作假
            }
        }
        return null;
    }

    /**
     * @return the VNU
     */
    public final ObjectId getVNU() {
        return VNU;
    }
}
