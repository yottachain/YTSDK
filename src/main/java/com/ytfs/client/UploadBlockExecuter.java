package com.ytfs.client;

import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.Block;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.node.SuperNodeList;
import com.ytfs.common.tracing.GlobalTracer;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.IOException;
import org.apache.log4j.Logger;

public class UploadBlockExecuter implements Runnable {

    private static final Logger LOG = Logger.getLogger(UploadBlockExecuter.class);

    private final UploadObject uploadObject;
    private final Block b;
    private final short blocknum;

    UploadBlockExecuter(UploadObject uploadObject, Block b, short ii) {
        this.uploadObject = uploadObject;
        this.b = b;
        this.blocknum = ii;
        synchronized (uploadObject.execlist) {
            uploadObject.execlist.add(this);
        }
    }

    private void execute() throws IOException, ServiceException, InterruptedException {
        b.calculate();
        if (b.getRealSize() > UserConfig.Default_Block_Size) {
            LOG.fatal("[" + uploadObject.VNU + "]Block length too large.");
        }
        SuperNode node = SuperNodeList.getBlockSuperNode(b.getVHP());
        LOG.info("[" + uploadObject.VNU + "]Start upload block " + blocknum + " to sn " + node.getId() + "...");
        Tracer tracer = GlobalTracer.getTracer();
        if (tracer != null) {
            Span span = tracer.buildSpan("UploadBlock").start();
            try (Scope scope = tracer.scopeManager().activate(span)) {
                uploadObject.upload(b, blocknum, node);
            } catch (Exception ex) {
                Tags.ERROR.set(span, true);
                throw ex instanceof ServiceException ? (ServiceException) ex : new ServiceException(SERVER_ERROR, ex.getMessage());
            } finally {
                span.finish();
            }
        } else {
            uploadObject.upload(b, blocknum, node);
        }
    }

    @Override
    public void run() {
        try {
            execute();
            free(null);
        } catch (ServiceException se) {
            LOG.error("[" + uploadObject.VNU + "]Block " + blocknum + " Upload ERR:" + se.getErrorCode());
            free(se);
        } catch (Throwable se) {
            LOG.error("[" + uploadObject.VNU + "]Block " + blocknum + " Upload ERR:", se);
            free(new ServiceException(SERVER_ERROR, se.getMessage()));
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
            uploadObject.execlist.notifyAll();
        }
    }

}
