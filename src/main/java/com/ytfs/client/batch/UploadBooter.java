package com.ytfs.client.batch;

import com.ytfs.client.ClientInitor;
import static com.ytfs.client.batch.UploadFile.UPLOAD_LOOP;
import com.ytfs.common.conf.UserConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class UploadBooter implements WrapperListener {

    private static final Logger LOG = Logger.getLogger(UploadBooter.class);

    public static int uploadFileThreadNum = 1;

    private static final List<UploadFile> list = new ArrayList();
    public static final AtomicLong total = new AtomicLong(0);
    private static long startTime = 0;

    private static void addList(UploadFile ufile) {
        synchronized (list) {
            list.add(ufile);
        }
    }

    public static void delList(UploadFile ufile) {
        synchronized (list) {
            list.remove(ufile);
            if (list.isEmpty()) {
                LOG.info("上传完毕!");
                LOG.info("线程数:" + uploadFileThreadNum);
                LOG.info("上传文件数:" + UPLOAD_LOOP * uploadFileThreadNum);
                long l = (System.currentTimeMillis() - startTime) / 1000L;
                long speed = total.get() / 1024L / l;
                LOG.info("上传总数据量:" + total.get() / 1024L / 1024L + "M,耗时:" + Long.toString(l) + "秒,平均" + speed + "KB/s");
            }
        }
    }

    @Override
    public Integer start(String[] strings) {
        //System.setProperty("snlist.conf", "conf/snlistYF.properties");
        //System.setProperty("ytfs.conf", "conf/ytfsYF.properties");
        try {
            String num = WrapperManager.getProperties().getProperty("wrapper.batch.uploadFileThreadNum", "5");
            try {
                uploadFileThreadNum = Integer.parseInt(num);
            } catch (Exception d) {
            }
            ClientInitor.init();
            UploadFackBlockExecuter.init();
            UploadFackShard.init();
            startTime = System.currentTimeMillis();
            for (int ii = 0; ii < uploadFileThreadNum; ii++) {
                UploadFile ufile = new UploadFile("bucket" + ii, UserConfig.tmpFilePath.getPath());
                addList(ufile);
                ufile.start();
            }
        } catch (Exception r) {
            System.exit(0);
        }
        return null;
    }

    @Override
    public int stop(int exitCode) {
        for (UploadFile ufile : list) {
            ufile.interrupt();
        }
        ClientInitor.stop();
        return exitCode;
    }

    public static void main(String[] args) {
        WrapperManager.start(new UploadBooter(), args);
    }

    @Override
    public void controlEvent(int event) {
        if (WrapperManager.isControlledByNativeWrapper() == false) {
            if (event == WrapperManager.WRAPPER_CTRL_C_EVENT
                    || event == WrapperManager.WRAPPER_CTRL_CLOSE_EVENT
                    || event == WrapperManager.WRAPPER_CTRL_SHUTDOWN_EVENT) {
                WrapperManager.stop(0);
            }
        }
    }
}
