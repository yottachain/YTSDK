package com.ytfs.client.examples;

import com.ytfs.client.ClientInitor;
import com.ytfs.client.DownloadObject;
import com.ytfs.client.UploadObject;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class SDKTest implements WrapperListener {

    @Override
    public Integer start(String[] strings) {
        try {
            ClientInitor.init();
            if (strings.length != 1) {
                throw new IOException("请指定要上传的文件!");
            }
            UploadObject upload = new UploadObject(strings[0]);
            byte[] VHW = upload.upload();
            System.out.println("上传完毕！准备下载......");

            String newfilepath = strings[0];
            int index = newfilepath.lastIndexOf(".");
            if (index > 0) {
                newfilepath = newfilepath.substring(0, index) + ".0" + newfilepath.substring(index);
            } else {
                newfilepath = newfilepath + ".0";
            }
            DownloadObject obj = new DownloadObject(VHW);
            FileOutputStream out = new FileOutputStream(newfilepath);
            InputStream is = obj.load();
            byte[] bs = new byte[1024 * 128];
            int len;
            while ((len = is.read(bs)) != -1) {
                out.write(bs, 0, len);
            }
            out.close();
            System.out.println("文件下载完毕，保存在：" + newfilepath);
        } catch (Exception r) {
            r.printStackTrace();
        }
        return null;
    }

    @Override
    public int stop(int exitCode) {
        ClientInitor.stop();
        return exitCode;
    }

    public static void main(String[] args) {
        WrapperManager.start(new SDKTest(), args);
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
