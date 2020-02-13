package com.ytfs.client;

import com.ytfs.common.GlobleThreadPool;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.Block;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.node.SuperNodeList;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;

public class UploadBlockExecuter implements Runnable {

    private static final Logger LOG = Logger.getLogger(UploadBlockExecuter.class);
    private static LinkedBlockingQueue<UploadBlockExecuter> BLOCK_POOL = null;

    public static void init() {
        if (BLOCK_POOL == null) {
            BLOCK_POOL = new LinkedBlockingQueue(UserConfig.UPLOADBLOCKTHREAD);
            for (int ii = 0; ii < UserConfig.UPLOADBLOCKTHREAD; ii++) {
                BLOCK_POOL.add(new UploadBlockExecuter());
            }
        }
    }

    static void startUploadBlock(UploadObject uploadObject, Block b, short ii) throws InterruptedException {
        UploadBlockExecuter uploader = BLOCK_POOL.take();
        uploader.uploadObject = uploadObject;
        uploader.b = b;
        uploader.blocknum = ii;
        synchronized (uploadObject.execlist) {
            uploadObject.execlist.add(uploader);
        }
        GlobleThreadPool.execute(uploader);
    }

    private UploadObject uploadObject;
    private Block b;
    private short blocknum;

    private void execute() throws IOException, ServiceException, InterruptedException {
        b.calculate();
        if (b.getRealSize() > UserConfig.Default_Block_Size) {
            LOG.fatal("[" + uploadObject.VNU + "][" + blocknum + "]Block length too large.");
        }
        SuperNode node = SuperNodeList.getBlockSuperNode(b.getVHP());
        LOG.info("[" + uploadObject.VNU + "][" + blocknum + "]Start upload block to sn " + node.getId() + "...");
        uploadObject.upload(b, blocknum, node);
    }

    @Override
    public void run() {
        try {
            execute();
            uploadObject.uploadedSize.addAndGet(b.getRealSize());
            free(null);
            LOG.info("[" + uploadObject.VNU + "]Upload object " + uploadObject.getProgress() + "%");
        } catch (ServiceException se) {
            LOG.error("[" + uploadObject.VNU + "][" + blocknum + "]Upload block ERR:" + se.getErrorCode());
            free(se);
        } catch (Throwable se) {
            LOG.error("[" + uploadObject.VNU + "][" + blocknum + "]Upload block ERR:", se);
            free(new ServiceException(SERVER_ERROR, se.getMessage()));
        } finally {
            BLOCK_POOL.add(this);
        }
    }

    private void free(ServiceException se) {
        synchronized (uploadObject.execlist) {
            uploadObject.execlist.remove(this);
            if (se != null) {
                uploadObject.err = se;
            } else {
                uploadObject.startTime = System.currentTimeMillis();
            }
            uploadObject.execlist.notify();
        }
    }

}
