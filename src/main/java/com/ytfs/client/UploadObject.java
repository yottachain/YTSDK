package com.ytfs.client;

import com.ytfs.client.v2.YTClient;
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
import com.ytfs.service.packet.v2.ActiveCacheV2;
import com.ytfs.service.packet.v2.UploadObjectInitReqV2;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

public class UploadObject extends UploadObjectAbstract {
    
    private static final Logger LOG = Logger.getLogger(UploadObject.class);
    private YTFileEncoder ytfile;
    ServiceException err = null;
    AtomicLong uploadedSize = new AtomicLong(0);
    long startTime;
    private byte[] data = null;
    private String path;
    private boolean exist = false;

    /**
     * 创建实例
     *
     * @param client
     * @param data 要上传的文件内容
     * @throws IOException
     */
    public UploadObject(YTClient client, byte[] data) throws IOException {
        this.client = client;
        this.data = data;
    }

    /**
     * 创建实例
     *
     * @param client
     * @param path 要上传的文件的本地路径
     * @throws IOException
     */
    public UploadObject(YTClient client, String path) throws IOException {
        this.client = client;
        this.path = path;
    }

    /**
     * 创建实例
     *
     * @param data 要上传的文件内容
     * @throws IOException
     */
    public UploadObject(byte[] data) throws IOException {
        this.data = data;
    }

    /**
     * 创建实例
     *
     * @param path 要上传的文件的本地路径
     * @throws IOException
     */
    public UploadObject(String path) throws IOException {
        this.path = path;
    }

    /**
     * 上传过程中可获取到当前文件的上传进度
     *
     * @return 0-100
     */
    public int getProgress() {
        if (exist) {
            return 100;
        }
        if (ytfile == null) {
            return 0;
        }
        long p = ytfile.getReadinTotal() * 100L / ytfile.getLength();
        long uploaded = ytfile.getOutTotal() == 0 ? 0 : (uploadedSize.get() * 100L / ytfile.getOutTotal());
        p = p * uploaded / 100L;
        return (int) p;
    }

    /**
     * 开始上传
     *
     * @return
     * @throws ServiceException
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public byte[] upload() throws ServiceException, IOException, InterruptedException {
        try {
            if (data != null) {
                ytfile = new YTFileEncoder(data);
            } else {
                ytfile = new YTFileEncoder(path);
            }
            this.VHW = ytfile.getVHW();
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
        } finally {
            if (ytfile != null) {
                ytfile.closeFile();
            }
        }
    }
    
    public byte[] uploadTracer() throws ServiceException, IOException, InterruptedException {
        UploadObjectInitResp res;
        if (this.client == null) {
            UploadObjectInitReq req = new UploadObjectInitReq(VHW);
            req.setLength(ytfile.getLength());
            res = (UploadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
        } else {
            UploadObjectInitReqV2 req = new UploadObjectInitReqV2(VHW);
            req.setLength(ytfile.getLength());
            req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
            res = (UploadObjectInitResp) P2PUtils.requestBPU(req, client.getSuperNode(), UserConfig.SN_RETRYTIMES);
        }
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
                        long curmem = memorys + b.getRealSize();
                        while (curmem >= UserConfig.UPLOADFILEMAXMEMORY) {
                            execlist.wait(15000);
                            if (System.currentTimeMillis() - startTime > 60000) {
                                sendActive();
                                startTime = System.currentTimeMillis();
                            }
                        }
                        if (err != null) {
                            throw err;
                        }
                    }
                    UploadBlockExecuter.startUploadBlock(this, b, ii);
                } else {
                    uploadedSize.addAndGet(b.getRealSize());
                }
                ii++;
            }
            synchronized (execlist) {
                while (execlist.size() > 0) {
                    execlist.wait(15000);
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
            LOG.info("[" + VNU + "]Upload object " + this.getProgress() + "%");
        } else {
            exist = true;
            LOG.info("[" + VNU + "]Already exists.");
        }
        return VHW;
    }
    
    private void sendActive() {
        try {
            if (this.client == null) {
                ActiveCache active = new ActiveCache();
                active.setVNU(VNU);
                P2PUtils.requestBPU(active, UserConfig.superNode, VNU.toString(), 0);
            } else {
                ActiveCacheV2 active = new ActiveCacheV2();
                active.setVNU(VNU);
                active.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
                P2PUtils.requestBPU(active, client.getSuperNode(), VNU.toString(), 0);
            }
        } catch (Exception r) {
        }
    }
}
