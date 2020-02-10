package com.ytfs.client.batch;

import com.ytfs.client.ClientInitor;
import com.ytfs.common.conf.UserConfig;
import java.util.ArrayList;
import java.util.List;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class UploadBooter implements WrapperListener {

    public static int uploadFileThreadNum = 1;

    private final List<UploadFile> list = new ArrayList();

    @Override
    public Integer start(String[] strings) {
        try {
            String num = WrapperManager.getProperties().getProperty("wrapper.batch.uploadFileThreadNum", "1");
            try {
                uploadFileThreadNum = Integer.getInteger(num);
            } catch (Exception d) {
            }
            ClientInitor.init();
            for (int ii = 0; ii < uploadFileThreadNum; ii++) {
                UploadFile ufile = new UploadFile("bucket" + ii, UserConfig.tmpFilePath.getPath());
                list.add(ufile);
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
