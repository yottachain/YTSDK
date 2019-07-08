package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.codec.Block;
import com.ytfs.common.codec.YTFile;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.common.ServiceException;
import com.ytfs.service.packet.UploadObjectInitReq;
import com.ytfs.service.packet.UploadObjectInitResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import java.util.List;
import org.apache.log4j.Logger;

public class UploadObjectSlow extends UploadObjectAbstract {

    private static final Logger LOG = Logger.getLogger(UploadObjectSlow.class);

    private final YTFile ytfile;

    public UploadObjectSlow(byte[] data) throws IOException {
        ytfile = new YTFile(data);
        this.VHW = ytfile.getVHW();
    }

    public UploadObjectSlow(String path) throws IOException {
        ytfile = new YTFile(path);
        this.VHW = ytfile.getVHW();
    }

    @Override
    public byte[] upload() throws ServiceException, IOException, InterruptedException {
        UploadObjectInitReq req = new UploadObjectInitReq(VHW);
        req.setLength(ytfile.getLength());
        UploadObjectInitResp res = (UploadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        VNU = res.getVNU();
        LOG.info("[" + VNU + "]Start upload object...");
        if (!res.isRepeat()) {
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
                            LOG.info("[" + VNU + "]Block " + ii + " has been uploaded.");
                            uploaded = true;
                            break;
                        }
                    }
                }
                if (!uploaded) {
                    b.calculate();
                    if (b.getRealSize() > UserConfig.Default_Block_Size) {
                        LOG.fatal("[" + VNU + "]Block length too large.");
                    }
                    SuperNode node = SuperNodeList.getBlockSuperNode(b.getVHP());
                    LOG.info("[" + VNU + "]Start upload block " + ii + " to sn " + node.getId() + "...");
                    int errtimes = 0;
                    for (;;) {
                        try {
                            upload(b, ii, node);
                            break;
                        } catch (ServiceException e) {
                            errtimes++;
                            if (errtimes < 3) {
                                Thread.sleep(5000);
                            } else {
                                throw e;
                            }
                        }
                    }
                }
                ii++;
            }
            complete();
            LOG.info("[" + VNU + "]Upload object OK.");
            ytfile.clear();
        } else {
            LOG.info("[" + VNU + "]Already exists.");
        }
        return VHW;
    }
}
