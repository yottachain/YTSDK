package com.ytfs.client;

import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.Block;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.node.SuperNodeList;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import org.apache.log4j.Logger;

public class UploadBlockExecuter implements Runnable {

    private static final Logger LOG = Logger.getLogger(UploadBlockExecuter.class);

    private final UploadObject uploadObject;
    private final Block b;
    private final short ii;

    UploadBlockExecuter(UploadObject uploadObject, Block b, short ii) {
        this.uploadObject = uploadObject;
        this.b = b;
        this.ii = ii;
        synchronized (uploadObject.execlist) {
            uploadObject.execlist.add(this);
        }
    }

    private void execute() throws IOException, InterruptedException, ServiceException {
        b.calculate();
        if (b.getRealSize() > UserConfig.Default_Block_Size) {
            LOG.fatal("[" + uploadObject.VNU + "]Block length too large.");
        }
        SuperNode node = SuperNodeList.getBlockSuperNode(b.getVHP());
        LOG.info("[" + uploadObject.VNU + "]Start upload block " + ii + " to sn " + node.getId() + "...");
        int errtimes = 0;
        for (;;) {
            try {
                uploadObject.upload(b, ii, node);
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

    @Override
    public void run() {
        try {
            execute();
            free(null);
        } catch (ServiceException se) {
            LOG.error("[" + uploadObject.VNU + "]Block " + ii + " Upload ERR:" + se.getErrorCode());
            free(se);
        } catch (Throwable se) {
            LOG.error("[" + uploadObject.VNU + "]Block " + ii + " Upload ERR:" + se.getMessage());
            free(new ServiceException(SERVER_ERROR, se.getMessage()));
        }
    }

    private void free(ServiceException se) {
        synchronized (uploadObject.execlist) {
            uploadObject.execlist.remove(this);
            if (se != null) {
                uploadObject.err = se;
            }
            uploadObject.execlist.notifyAll();
        }
    }

}
