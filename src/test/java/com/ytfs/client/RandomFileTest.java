package com.ytfs.client;

import com.ytfs.client.examples.MakeRandFile;
import com.ytfs.common.ServiceException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import org.apache.commons.codec.binary.Hex;

public class RandomFileTest {

    private static final String sn = "cs";

    public static void main(String[] args) throws IOException {
        if (sn.equalsIgnoreCase("cs")) {
            System.setProperty("snlist.conf", "conf/snlist_CS.properties");
            System.setProperty("ytfs.conf", "conf/ytfs.properties");
        } else {
            System.setProperty("snlist.conf", "conf/snlist_YF.properties");
            System.setProperty("ytfs.conf", "conf/ytfs.properties");
        }
        ClientInitor.init();
        //测试小文件
        //test(new UploadObject(MakeRandFile.makeSmallFile()));
        //测试多副本
        //test(new UploadObject(MakeRandFile.makeMediumFile()));
        //测试rs模式
        test(new UploadObject(MakeRandFile.makeLargeFile()));
    }

    public static void test(UploadObject upload) {
        try {
            byte[] VHW = upload.upload();
            System.out.println(Hex.encodeHexString(VHW) + " 上传完毕！准备下载......");

            DownloadObject obj = new DownloadObject(VHW);
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            InputStream is = obj.load(0, 1111111111111L);
            byte[] bs = new byte[1024 * 128];
            int len;
            while ((len = is.read(bs)) != -1) {
                sha256.update(bs, 0, len);
            }
            byte[] VHW1 = sha256.digest();
            System.out.println(Hex.encodeHexString(VHW1) + " 文件下载完毕!");
        } catch (ServiceException se) {
            se.printStackTrace();
            System.out.println(Integer.toHexString(se.getErrorCode()));
        } catch (Exception r) {
            r.printStackTrace();
        }
    }
}
