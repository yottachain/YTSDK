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
import com.ytfs.service.packet.SubBalanceReq;
import com.ytfs.service.packet.UploadBlockDBReq;
import com.ytfs.service.packet.UploadBlockDupReq;
import com.ytfs.service.packet.UploadBlockDupResp;
import com.ytfs.service.packet.UploadBlockInit2Req;
import com.ytfs.service.packet.UploadBlockInitReq;
import com.ytfs.service.packet.UploadBlockInitResp;
import com.ytfs.service.packet.UploadObjectEndReq;
import com.ytfs.service.packet.UploadObjectEndResp;
import com.ytfs.service.packet.VoidResp;
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

    public abstract byte[] upload() throws ServiceException, IOException, InterruptedException;

    public final byte[] getVHW() {
        return VHW;
    }

    //结束上传
    protected final void complete() throws ServiceException {
        UploadObjectEndReq req = new UploadObjectEndReq();
        req.setVHW(VHW);
        UploadObjectEndResp resp = (UploadObjectEndResp) P2PUtils.requestBPU(req, UserConfig.superNode, VNU.toString());
        byte[] bs = resp.getSignArg();
        SubBalanceReq sub = new SubBalanceReq();
        sub.setVNU(VNU);
        try {
            byte[] signData = EOSRequest.makeSubBalanceRequest(bs, UserConfig.username,
                    UserConfig.privateKey, resp.getContractAccount(), resp.getFirstCost(), resp.getUserid());
            sub.setSignData(signData);
        } catch (Exception e) {
            throw new ServiceException(SERVER_ERROR);
        }
        P2PUtils.requestBPU(sub, UserConfig.superNode, VNU.toString());
    }

    //上传块
    protected final void upload(Block b, short id, SuperNode node) throws ServiceException, IOException, InterruptedException {
        BlockEncrypted be = new BlockEncrypted(b.getRealSize());
        UploadBlockInitReq req = new UploadBlockInitReq(VNU, b.getVHP(), be.getShardCount(), id);
        Object resp = P2PUtils.requestBPU(req, node, VNU.toString());
        if (resp instanceof VoidResp) {//已经上传
            LOG.info("[" + VNU + "]Block " + id + " is being uploaded.");
            return;
        }
        if (resp instanceof UploadBlockDupResp) {//重复,resp.getExist()=0已经上传     
            UploadBlockDupReq uploadBlockDupReq = checkResp((UploadBlockDupResp) resp, b);
            if (uploadBlockDupReq != null) {//请求节点
                uploadBlockDupReq.setId(id);
                uploadBlockDupReq.setVHP(b.getVHP());  //计数
                uploadBlockDupReq.setOriginalSize(b.getOriginalSize());
                uploadBlockDupReq.setRealSize(b.getRealSize());
                uploadBlockDupReq.setVNU(VNU);
                P2PUtils.requestBPU(uploadBlockDupReq, node, VNU.toString());
                LOG.info("[" + VNU + "]Block " + id + " is a repetitive block:" + Base58.encode(b.getVHP()));
            } else {
                if (!be.needEncode()) {
                    UploadBlockToDB(b, id, node);
                } else {//请求分配节点
                    UploadBlockInit2Req req2 = new UploadBlockInit2Req(req);
                    UploadBlockInitResp resp1 = (UploadBlockInitResp) P2PUtils.requestBPU(req2, node, VNU.toString());
                    UploadBlock ub = new UploadBlock(b, id, resp1.getNodes(), resp1.getVBI(), node, VNU);
                    ub.upload();
                }
            }
        }
        if (resp instanceof UploadBlockInitResp) {
            if (!be.needEncode()) {
                UploadBlockToDB(b, id, node);
            } else {
                UploadBlockInitResp resp1 = (UploadBlockInitResp) resp;
                UploadBlock ub = new UploadBlock(b, id, resp1.getNodes(), resp1.getVBI(), node, VNU);
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
            P2PUtils.requestBPU(req, node, VNU.toString());
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
