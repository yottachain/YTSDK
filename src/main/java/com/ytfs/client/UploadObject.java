package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.codec.Block;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.YTFileEncoder;
import com.ytfs.service.packet.UploadObjectInitReq;
import com.ytfs.service.packet.UploadObjectInitResp;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import org.apache.log4j.Logger;

public class UploadObject extends UploadObjectAbstract {

    private static final Logger LOG = Logger.getLogger(UploadObject.class);

    private final YTFileEncoder ytfile;

    public UploadObject(byte[] data) throws IOException {
        ytfile = new YTFileEncoder(data);
        this.VHW = ytfile.getVHW();
    }

    public UploadObject(String path) throws IOException {
        ytfile = new YTFileEncoder(path);
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
            short[] refers = res.getBlocks();
            short ii = 0;
            while (!ytfile.isFinished()) {
                Block b = ytfile.handle();
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
        } else {
            LOG.info("[" + VNU + "]Already exists.");
        }
        return VHW;
    }
}
