package com.ytfs.client;

import com.ytfs.client.v2.PreAllocNodeMgrV2;
import com.ytfs.client.v2.YTClient;
import static com.ytfs.common.ServiceErrorCode.DN_IN_BLACKLIST;
import static com.ytfs.common.ServiceErrorCode.INVALID_UPLOAD_ID;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.Block;
import com.ytfs.common.codec.BlockAESEncryptor;
import com.ytfs.common.codec.BlockEncrypted;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.codec.ShardEncoder;
import com.ytfs.common.codec.erasure.ShardRSEncoder;
import com.ytfs.common.codec.lrc.ShardLRCEncoder;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.user.UploadBlockDBReq;
import com.ytfs.service.packet.user.UploadBlockDupReq;
import com.ytfs.service.packet.user.UploadBlockDupResp;
import com.ytfs.service.packet.user.UploadBlockInitReq;
import com.ytfs.service.packet.user.UploadBlockInitResp;
import com.ytfs.service.packet.user.UploadObjectEndReq;
import com.ytfs.service.packet.v2.UploadBlockDBReqV2;
import com.ytfs.service.packet.v2.UploadBlockDupReqV2;
import com.ytfs.service.packet.v2.UploadBlockInitReqV2;
import com.ytfs.service.packet.v2.UploadObjectEndReqV2;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public abstract class UploadObjectAbstract {

    private static final Logger LOG = Logger.getLogger(UploadObjectAbstract.class);

    protected ObjectId VNU;
    protected byte[] VHW;
    protected String signArg;
    protected long stamp;
    protected long memorys = 0;
    protected final List<UploadBlockExecuter> execlist = new LinkedList<>();
    protected YTClient client = null;

    public abstract byte[] upload() throws ServiceException, IOException, InterruptedException;

    public final byte[] getVHW() {
        return VHW;
    }

    //结束上传
    protected final void complete() throws ServiceException {
        try {
            if (client == null) {
                UploadObjectEndReq req = new UploadObjectEndReq();
                req.setVHW(VHW);
                req.setVNU(VNU);
                P2PUtils.requestBPU(req, UserConfig.superNode, VNU.toString(), UserConfig.SN_RETRYTIMES);
            } else {
                UploadObjectEndReqV2 req = new UploadObjectEndReqV2();
                req.setVHW(VHW);
                req.setVNU(VNU);
                req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
                P2PUtils.requestBPU(req, client.getSuperNode(), VNU.toString(), UserConfig.SN_RETRYTIMES);
            }
        } catch (ServiceException e) {
            if (e.getErrorCode() != INVALID_UPLOAD_ID) {
                throw e;
            }
        }
    }

    protected void memoryChange(long len) {
        synchronized (execlist) {
            long curmem = memorys + len;
            if (curmem < UserConfig.UPLOADFILEMAXMEMORY) {
                execlist.notify();
            }
        }
    }

    //上传块
    public final void upload(Block b, short id, SuperNode node) throws ServiceException, InterruptedException {
        for (int ii = 0; ii < 3; ii++) {
            try {
                uploadNoCheck(b, id, node);
                return;
            } catch (ServiceException se) {
                if (se.getErrorCode() == DN_IN_BLACKLIST) {
                    LOG.info("[" + VNU + "][" + id + "]Upload block ERR:DN_IN_BLACKLIST");
                    PreAllocNodeMgrV2.Reset();
                    PreAllocNodeMgr.Reset();
                    Thread.sleep(15000);
                    continue;
                }
                throw se;
            }
        }
        throw new ServiceException(DN_IN_BLACKLIST);
    }

    public final void uploadNoCheck(Block b, short id, SuperNode node) throws ServiceException, InterruptedException {
        long l = System.currentTimeMillis();
        BlockEncrypted be = new BlockEncrypted(b.getRealSize());
        Object resp;
        if (this.client == null) {
            UploadBlockInitReq req = new UploadBlockInitReq(VNU, b.getVHP(), id);
            resp = P2PUtils.requestBPU(req, node, VNU.toString(), UserConfig.SN_RETRYTIMES);
        } else {
            UploadBlockInitReqV2 req = new UploadBlockInitReqV2(VNU, b.getVHP(), id);
            req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
            req.setVersion(Version.getVersionID());
            resp = P2PUtils.requestBPU(req, node, UserConfig.SN_RETRYTIMES);
        }
        if (resp instanceof UploadBlockDupResp) {//重复,resp.getExist()=0已经上传  
            Object obj = checkResp((UploadBlockDupResp) resp, b);
            if (obj != null) {//请求节点
                b.clearData();
                this.memoryChange(b.getRealSize() * -1);
                if (obj instanceof UploadBlockDupReq) {
                    UploadBlockDupReq uploadBlockDupReq = (UploadBlockDupReq) obj;
                    uploadBlockDupReq.setId(id);
                    uploadBlockDupReq.setVHP(b.getVHP());  //计数
                    uploadBlockDupReq.setOriginalSize(b.getOriginalSize());
                    uploadBlockDupReq.setRealSize(b.getRealSize());
                    uploadBlockDupReq.setVNU(VNU);
                    P2PUtils.requestBPU(uploadBlockDupReq, node, VNU.toString(), UserConfig.SN_RETRYTIMES);
                } else {
                    UploadBlockDupReqV2 uploadBlockDupReq = (UploadBlockDupReqV2) obj;
                    uploadBlockDupReq.setId(id);
                    uploadBlockDupReq.setVHP(b.getVHP());  //计数
                    uploadBlockDupReq.setOriginalSize(b.getOriginalSize());
                    uploadBlockDupReq.setRealSize(b.getRealSize());
                    uploadBlockDupReq.setVNU(VNU);
                    P2PUtils.requestBPU(uploadBlockDupReq, node, VNU.toString(), UserConfig.SN_RETRYTIMES);
                }
                LOG.info("[" + VNU + "][" + id + "]Block is a repetitive block:" + Base58.encode(b.getVHP()));
            } else {
                if (!be.needEncode()) {
                    UploadBlockToDB(b, id, node);
                    LOG.info("[" + VNU + "][" + id + "]Block is uploaded to DB:" + Base58.encode(b.getVHP()));
                    b.clearData();
                    this.memoryChange(b.getRealSize() * -1);
                } else {//请求分配节点
                    UploadBlock ub = new UploadBlock(this, b, id, node, VNU, ((UploadBlockDupResp) resp).getStartTime(), signArg, stamp);
                    LOG.info("[" + VNU + "][" + id + "]Block is initialized at sn " + node.getId() + ",take times "
                            + (System.currentTimeMillis() - l) + "ms," + Base58.encode(b.getVHP()));
                    ub.upload();
                }
            }
        }
        if (resp instanceof UploadBlockInitResp) {
            if (!be.needEncode()) {
                UploadBlockToDB(b, id, node);
                LOG.info("[" + VNU + "][" + id + "]Block is uploaded to DB:" + Base58.encode(b.getVHP()));
                b.clearData();
                this.memoryChange(b.getRealSize() * -1);
            } else {
                UploadBlock ub = new UploadBlock(this, b, id, node, VNU, ((UploadBlockInitResp) resp).getStartTime(), signArg, stamp);
                LOG.info("[" + VNU + "][" + id + "]Block is initialized at sn " + node.getId() + ",take times "
                        + (System.currentTimeMillis() - l) + "ms," + Base58.encode(b.getVHP()));
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
            if (this.client == null) {
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
            } else {
                UploadBlockDBReqV2 req = new UploadBlockDBReqV2();
                req.setId(id);
                req.setVNU(VNU);
                req.setVHP(b.getVHP());
                req.setVHB(enc.getBlockEncrypted().getVHB());
                req.setKEU(KeyStoreCoder.aesEncryped(ks, client.getAESKey()));
                req.setKED(KeyStoreCoder.aesEncryped(ks, b.getKD()));
                req.setOriginalSize(b.getOriginalSize());
                req.setData(enc.getBlockEncrypted().getData());
                req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
                P2PUtils.requestBPU(req, node, VNU.toString(), UserConfig.SN_RETRYTIMES);
            }
            LOG.info("[" + VNU + "][" + id + "]Upload block to DB,VHP:" + Base58.encode(b.getVHP()));
        } catch (Throwable e) {
            LOG.error("[" + VNU + "][" + id + "]Upload block ERR:" + e.getMessage());
            throw e instanceof ServiceException ? (ServiceException) e : new ServiceException(SERVER_ERROR, e.getMessage());
        }
    }

    //检查重复
    private Object checkResp(UploadBlockDupResp resp, Block b) {
        byte[][] keds = resp.getKED();
        byte[][] vhbs = resp.getVHB();
        int[] ars = resp.getAR();
        for (int ii = 0; ii < keds.length; ii++) {
            byte[] ked = keds[ii];
            try {
                byte[] ks = KeyStoreCoder.aesDecryped(ked, b.getKD());
                byte[] VHB;
                BlockAESEncryptor aes = new BlockAESEncryptor(b, ks);
                aes.encrypt();
                if (aes.getBlockEncrypted().needEncode()) {
                    if (ars[ii] == ShardEncoder.AR_RS_MODE) {
                        ShardRSEncoder enc = new ShardRSEncoder(aes.getBlockEncrypted());
                        enc.encode();
                        VHB = enc.makeVHB();
                    } else {
                        ShardLRCEncoder enc = new ShardLRCEncoder(aes.getBlockEncrypted());
                        enc.encode();
                        VHB = enc.makeVHB();
                    }
                } else {
                    VHB = aes.getBlockEncrypted().getVHB();
                }
                if (Arrays.equals(vhbs[ii], VHB)) {
                    if (this.client == null) {
                        UploadBlockDupReq req = new UploadBlockDupReq();
                        req.setVHB(VHB);
                        byte[] keu = KeyStoreCoder.aesEncryped(ks, UserConfig.AESKey);
                        req.setKEU(keu);
                        return req;
                    } else {
                        UploadBlockDupReqV2 req = new UploadBlockDupReqV2();
                        req.setVHB(VHB);
                        byte[] keu = KeyStoreCoder.aesEncryped(ks, client.getAESKey());
                        req.setKEU(keu);
                        req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
                        return req;
                    }
                }
            } catch (Exception r) {//解密不了,认为作假
            }
        }
        return null;
    }

    /**
     * 上传成功后，可以获取到该文件的唯一文件ID
     *
     * @return the VNU
     */
    public final ObjectId getVNU() {
        return VNU;
    }

}
