package com.ytfs.client;

import com.ytfs.common.GlobleThreadPool;
import static com.ytfs.common.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.codec.Block;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.YTFileEncoder;
import com.ytfs.common.tracing.GlobalTracer;
import com.ytfs.service.packet.user.UploadObjectInitReq;
import com.ytfs.service.packet.user.UploadObjectInitResp;
import com.ytfs.service.packet.bp.ActiveCache;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class UploadObject extends UploadObjectAbstract {

    private static final Logger LOG = Logger.getLogger(UploadObject.class);
    private final YTFileEncoder ytfile;
    final List<UploadBlockExecuter> execlist = new ArrayList();
    ServiceException err = null;
    long startTime;

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
        try {
            Tracer tracer = GlobalTracer.getTracer();
            if (tracer != null) {
                Span span = tracer.buildSpan("UploadObject").start();
                try (Scope scope = tracer.scopeManager().activate(span)) {
                    return uploadTracer();
                } catch (Exception ex) {
                    Tags.ERROR.set(span, true);
                    throw ex instanceof ServiceException ? (ServiceException) ex : new ServiceException(SERVER_ERROR, ex.getMessage());
                } finally {
                    span.finish();
                }
            } else {
                return uploadTracer();
            }
        } catch (ServiceException | IOException | InterruptedException r) {
            if (ytfile != null) {
                ytfile.closeFile();
            }
            throw r;
        }
    }

    public byte[] uploadTracer() throws ServiceException, IOException, InterruptedException {
        UploadObjectInitReq req = new UploadObjectInitReq(VHW);
        req.setLength(ytfile.getLength());
        UploadObjectInitResp res = (UploadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
        signArg = res.getSignArg();
        stamp = res.getStamp();
        VNU = res.getVNU();
        LOG.info("[" + VNU + "]Start upload object...");
        startTime = System.currentTimeMillis();
        if (!res.isRepeat()) {
            short[] refers = res.getBlocks();
            short ii = 0;
            while (!ytfile.isFinished()) {
                Block b = ytfile.handle();
                boolean uploaded = false;
                if (res.getBlocks() != null) { //检查是否已经上传
                    for (short refer : refers) {
                        if (ii == refer) {
                            LOG.info("[" + VNU + "][" + ii + "]Block has been uploaded.");
                            uploaded = true;
                            break;
                        }
                    }
                }
                if (err != null) {
                    throw err;
                }
                if (!uploaded) {
                    synchronized (execlist) {
                        while (execlist.size() >= UserConfig.UPLOADBLOCKTHREAD) {
                            execlist.wait(15000);
                            if (System.currentTimeMillis() - startTime > 60000) {
                                sendActive();
                                startTime = System.currentTimeMillis();
                            }
                        }
                        if (err != null) {
                            throw err;
                        }
                        UploadBlockExecuter exec = new UploadBlockExecuter(this, b, ii);
                        GlobleThreadPool.execute(exec);
                    }
                }
                ii++;
            }
            synchronized (execlist) {
                while (execlist.size() > 0) {
                    execlist.wait(5000);
                    if (System.currentTimeMillis() - startTime > 60000) {
                        sendActive();
                        startTime = System.currentTimeMillis();
                    }
                }
            }
            if (err != null) {
                throw err;
            }
            complete();
            LOG.info("[" + VNU + "]Upload object OK.");
        } else {
            LOG.info("[" + VNU + "]Already exists.");
        }
        return VHW;
    }

    private void sendActive() {
        try {
            ActiveCache active = new ActiveCache();
            active.setVNU(VNU);
            P2PUtils.requestBPU(active, UserConfig.superNode, VNU.toString(), 0);
        } catch (Exception r) {
        }
    }
}
