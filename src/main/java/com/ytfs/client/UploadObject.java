package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.codec.Block;
import com.ytfs.common.codec.BlockAESEncryptor;
import com.ytfs.common.codec.BlockEncrypted;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.codec.ShardRSEncoder;
import com.ytfs.common.codec.YTFile;
import com.ytfs.common.eos.EOSRequest;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.service.packet.GetBalanceReq;
import com.ytfs.service.packet.SubBalanceReq;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.UploadBlockDBReq;
import com.ytfs.service.packet.UploadBlockDupReq;
import com.ytfs.service.packet.UploadBlockDupResp;
import com.ytfs.service.packet.UploadBlockInit2Req;
import com.ytfs.service.packet.UploadBlockInitReq;
import com.ytfs.service.packet.UploadBlockInitResp;
import com.ytfs.service.packet.UploadObjectEndReq;
import com.ytfs.service.packet.UploadObjectEndResp;
import com.ytfs.service.packet.UploadObjectInitReq;
import com.ytfs.service.packet.UploadObjectInitResp;
import com.ytfs.service.packet.VoidResp;
import com.ytfs.service.packet.s3.UploadFileReq;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public class UploadObject {

    private static final Logger LOG = Logger.getLogger(UploadObject.class);

    private final YTFile ytfile;
    private ObjectId VNU;

    public UploadObject(byte[] data) throws IOException {
        ytfile = new YTFile(data);
    }

    public UploadObject(String path) throws IOException {
        ytfile = new YTFile(path);
    }

    public byte[] upload() throws ServiceException, IOException, InterruptedException {
        UploadObjectInitReq req = new UploadObjectInitReq(ytfile.getVHW());
        UploadObjectInitResp res = (UploadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        VNU = res.getVNU();
        if (!res.isRepeat()) {
            byte[] bs = res.getSignArg();
            byte[] signData = EOSRequest.makeGetBalanceRequest(bs, UserConfig.username, UserConfig.privateKey, UserConfig.contractAccount);
            GetBalanceReq getBalanceReq = new GetBalanceReq();
            getBalanceReq.setLength(ytfile.getLength());
            getBalanceReq.setSignData(signData);
            getBalanceReq.setVHW(ytfile.getVHW());
            getBalanceReq.setVNU(res.getVNU());
            P2PUtils.requestBPU(getBalanceReq, UserConfig.superNode);
            ytfile.init(res.getVNU().toHexString());
            ytfile.handle();
            List<Block> blockList = ytfile.getBlockList();
            short[] refers = res.getBlocks();
            short ii = 0;
            for (Block b : blockList) {
                try {
                    b.load();//出错需要重新分块
                } catch (IOException d) {
                    ytfile.clear();
                    throw d;
                }
                boolean uploaded = false;
                if (res.getBlocks() != null) { //检查是否已经上传
                    for (short refer : refers) {
                        if (ii == refer) {
                            uploaded = true;
                            break;
                        }
                    }
                }
                if (!uploaded) {
                    upload(b, res.getVNU(), ii);
                }
                ii++;
            }
            complete(ytfile.getVHW());
            ytfile.clear();
        }
        LOG.info("Upload object " + VNU);
        return ytfile.getVHW();
    }

    public byte[] getVHW() {
        return ytfile.getVHW();
    }

    //结束上传
    private void complete(byte[] VHW) throws ServiceException {
        UploadObjectEndReq req = new UploadObjectEndReq();
        req.setVHW(VHW);
        UploadObjectEndResp resp = (UploadObjectEndResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        byte[] bs = resp.getSignArg();
        SubBalanceReq sub = new SubBalanceReq();
        sub.setVNU(VNU);
        try {
            byte[] signData = EOSRequest.makeSubBalanceRequest(bs, UserConfig.username,
                    UserConfig.privateKey, UserConfig.contractAccount, resp.getFirstCost());
            sub.setSignData(signData);
        } catch (Exception e) {
            throw new ServiceException(SERVER_ERROR);
        }
        P2PUtils.requestBPU(sub, UserConfig.superNode);
    }

    //上传块
    private void upload(Block b, ObjectId vnu, short id) throws ServiceException, IOException, InterruptedException {
        b.calculate();
        SuperNode node = SuperNodeList.getBlockSuperNode(b.getVHP());
        BlockEncrypted be = new BlockEncrypted(b.getRealSize());
        UploadBlockInitReq req = new UploadBlockInitReq(vnu, b.getVHP(), be.getShardCount(), id);
        Object resp = P2PUtils.requestBPU(req, node);
        if (resp instanceof VoidResp) {//已经上传
            return;
        }
        if (resp instanceof UploadBlockDupResp) {//重复,resp.getExist()=0已经上传     
            UploadBlockDupReq uploadBlockDupReq = checkResp((UploadBlockDupResp) resp, b);
            if (uploadBlockDupReq != null) {//请求节点
                uploadBlockDupReq.setId(id);
                uploadBlockDupReq.setVHP(b.getVHP());  //计数
                uploadBlockDupReq.setOriginalSize(b.getOriginalSize());
                uploadBlockDupReq.setRealSize(b.getRealSize());
                uploadBlockDupReq.setVNU(vnu);
                P2PUtils.requestBPU(uploadBlockDupReq, node);
            } else {
                if (!be.needEncode()) {
                    UploadBlockToDB(b, vnu, id, node);
                } else {//请求分配节点
                    UploadBlockInit2Req req2 = new UploadBlockInit2Req(req);
                    UploadBlockInitResp resp1 = (UploadBlockInitResp) P2PUtils.requestBPU(req2, node);
                    UploadBlock ub = new UploadBlock(b, id, resp1.getNodes(), resp1.getVBI(), node);
                    ub.upload();
                }
            }
        }
        if (resp instanceof UploadBlockInitResp) {
            if (!be.needEncode()) {
                UploadBlockToDB(b, vnu, id, node);
            } else {
                UploadBlockInitResp resp1 = (UploadBlockInitResp) resp;
                UploadBlock ub = new UploadBlock(b, id, resp1.getNodes(), resp1.getVBI(), node);
                ub.upload();
            }
        }
    }

    //上传小文件至数据库
    private void UploadBlockToDB(Block b, ObjectId vnu, short id, SuperNode node) throws ServiceException {
        try {
            byte[] ks = KeyStoreCoder.generateRandomKey();
            BlockAESEncryptor enc = new BlockAESEncryptor(b, ks);
            enc.encrypt();
            UploadBlockDBReq req = new UploadBlockDBReq();
            req.setId(id);
            req.setVNU(vnu);
            req.setVHP(b.getVHP());
            req.setVHB(enc.getBlockEncrypted().getVHB());
            req.setKEU(KeyStoreCoder.eccEncryped(ks, UserConfig.KUEp));
            req.setKED(KeyStoreCoder.aesEncryped(ks, b.getKD()));
            req.setOriginalSize(b.getOriginalSize());
            req.setData(enc.getBlockEncrypted().getData());
            P2PUtils.requestBPU(req, node);
        } catch (Exception e) {
            LOG.error("", e);
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
                    byte[] keu = KeyStoreCoder.eccEncryped(ks, UserConfig.KUEp);
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
    public ObjectId getVNU() {
        return VNU;
    }

    public void writeMeta(String bucketname, String filename) throws ServiceException {
        UploadFileReq req = new UploadFileReq();
        req.setBucketname(bucketname);
        req.setFileName(filename);
        req.setVNU(VNU);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }
}
