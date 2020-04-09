package com.ytfs.client.batch;

import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.Block;
import com.ytfs.common.codec.BlockEncrypted;
import com.ytfs.common.conf.UserConfig;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

public abstract class UploadFackObjectAbstract {

    private static final Logger LOG = Logger.getLogger(UploadFackObjectAbstract.class);

    protected ObjectId VNU;
    protected byte[] VHW;
    protected String signArg;
    protected long stamp;
    protected long memorys = 0;
    protected final List<UploadFackBlockExecuter> execlist = new LinkedList<>();

    public abstract byte[] upload() throws ServiceException, IOException, InterruptedException;

    public final byte[] getVHW() {
        return VHW;
    }

    //结束上传
    protected final void complete() throws ServiceException {
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
        long l = System.currentTimeMillis();
        BlockEncrypted be = new BlockEncrypted(b.getRealSize());
        if (!be.needEncode()) {
            LOG.info("[" + VNU + "][" + id + "]Block is uploaded to DB:" + Base58.encode(b.getVHP()));
            b.clearData();
            this.memoryChange(b.getRealSize() * -1);
        } else {
            UploadFackBlock ub = new UploadFackBlock(this, b, id, node, VNU, System.currentTimeMillis(), signArg, stamp);
            LOG.info("[" + VNU + "][" + id + "]Block is initialized at sn " + node.getId() + ",take times "
                    + (System.currentTimeMillis() - l) + "ms," + Base58.encode(b.getVHP()));
            ub.upload();
        }
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
