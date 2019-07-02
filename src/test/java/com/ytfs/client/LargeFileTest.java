package com.ytfs.client;

import com.ytfs.common.ServiceException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import org.apache.commons.codec.binary.Hex;

public class LargeFileTest {

    private static final String filepath = "d:/DELPHI7企业版.ISO";

    public static void main(String[] args) {
        try {
            ClientInitor.init();
            UploadObjectSlow upload = new UploadObjectSlow(filepath);
            byte[] VHW = upload.upload();
            System.out.println(Hex.encodeHexString(VHW) + " 上传完毕！准备下载......");

            String newfilepath = filepath;
            int index = newfilepath.lastIndexOf(".");
            if (index > 0) {
                newfilepath = newfilepath.substring(0, index) + ".0" + newfilepath.substring(index);
            } else {
                newfilepath = newfilepath + ".0";
            }

            DownloadObject obj = new DownloadObject(VHW);
            FileOutputStream out = new FileOutputStream(newfilepath);
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            InputStream is = obj.load();
            byte[] bs = new byte[1024 * 128];
            int len;
            while ((len = is.read(bs)) != -1) {
                out.write(bs, 0, len);
                sha256.update(bs, 0, len);
            }
            out.close();
            byte[] VHW1 = sha256.digest();
            System.out.println(Hex.encodeHexString(VHW1) + " 文件下载完毕，保存在：" + newfilepath);
        } catch (ServiceException se) {
            se.printStackTrace();
            System.out.println(Integer.toHexString(se.getErrorCode()));
        } catch (Exception r) {
            r.printStackTrace();
        }
    }
}
