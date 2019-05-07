package com.ytfs.client;

import com.ytfs.service.packet.ServiceException;
import java.io.FileOutputStream;
import java.io.InputStream;

public class UploadTest {

    public static void main(String[] args) {
        try {
            ClientInitor.init();
            //UploadObject upload = new UploadObject("d:/LICENSE.txt");
            UploadObject upload = new UploadObject("F:/TortoiseGit-1.8.15.0-64bit.msi");            
            //byte[] VHW = upload.upload();
            byte[] VHW =upload.getVHW();
            //System.out.println(upload.getVNU());

            DownloadObject obj = new DownloadObject(VHW);
            //FileOutputStream out = new FileOutputStream("d:/LICENSE1.txt");
            FileOutputStream out=new FileOutputStream("F:/Tortoise.msi");
            InputStream is = obj.load();
            byte[] bs = new byte[1024*128];
            int len;
            while ((len = is.read(bs)) != -1) {
                out.write(bs, 0, len);
            }
            out.close();
        } catch (ServiceException se) {
            se.printStackTrace();
            System.out.println(Integer.toHexString(se.getErrorCode()));
        } catch (Exception r) {
            r.printStackTrace();
        }
    }
}
