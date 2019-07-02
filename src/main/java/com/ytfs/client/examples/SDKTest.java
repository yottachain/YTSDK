package com.ytfs.client.examples;

import com.ytfs.client.ClientInitor;
import com.ytfs.client.DownloadObject;
import com.ytfs.client.UploadObject;
import static com.ytfs.client.examples.MakeRandFile.largeFileLength;
import static com.ytfs.client.examples.MakeRandFile.mediumFileLength;
import static com.ytfs.client.examples.MakeRandFile.smallFileLength;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import org.apache.commons.codec.binary.Hex;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class SDKTest implements WrapperListener {

    @Override
    public Integer start(String[] strings) {
        try {
            ClientInitor.init();
            if (strings.length < 1) {
                strings = new String[]{"d:/ljhcode.rar"};
            }
            String filepath = null;
            String newfilepath = null;
            if (strings.length > 0) {
                File file = new File(strings[0]);
                if (file.exists() && file.isFile()) {
                    filepath = strings[0];
                    newfilepath = strings[0];
                    int index = newfilepath.lastIndexOf(".");
                    if (index > 0) {
                        newfilepath = newfilepath.substring(0, index) + ".0" + newfilepath.substring(index);
                    } else {
                        newfilepath = newfilepath + ".0";
                    }
                }
            }
            UploadObject upload;
            byte[] VHW = null;
            if (filepath != null) {
                System.out.println("准备上传文件:" + strings[0]);
                upload = new UploadObject(filepath);
            } else {
                int index = (int) System.currentTimeMillis() % 3;
                switch (index) {
                    case 0:
                        System.out.println("准备上传文件，大小(b):" + smallFileLength);
                        upload = new UploadObject(MakeRandFile.makeSmallFile());
                        break;
                    case 1:
                        System.out.println("准备上传文件，大小(b):" + mediumFileLength);
                        upload = new UploadObject(MakeRandFile.makeMediumFile());
                        break;
                    default:
                        System.out.println("准备上传文件，大小(b):" + largeFileLength);
                        upload = new UploadObject(MakeRandFile.makeLargeFile());
                        break;
                }
            }
            VHW = upload.upload();
            System.out.println(Hex.encodeHexString(VHW) + " 上传完毕！准备下载......");

            DownloadObject obj = new DownloadObject(VHW);
            FileOutputStream out = null;
            if (newfilepath != null) {
                out = new FileOutputStream(newfilepath);
            }
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            InputStream is = obj.load();
            byte[] bs = new byte[1024 * 128];
            int len;
            while ((len = is.read(bs)) != -1) {
                if (out != null) {
                    out.write(bs, 0, len);
                }
                sha256.update(bs, 0, len);
            }
            byte[] VHW1 = sha256.digest();
            if (out != null) {
                out.close();
                System.out.println(Hex.encodeHexString(VHW1) + " 文件下载完毕，保存在：" + newfilepath);
            } else {
                System.out.println(Hex.encodeHexString(VHW1) + " 文件下载完毕!");
            }
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
