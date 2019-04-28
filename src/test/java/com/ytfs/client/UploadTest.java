package com.ytfs.client;

import com.ytfs.service.packet.ServiceException;
import java.io.FileOutputStream;
import java.io.InputStream;

public class UploadTest {

    public static void main(String[] args) {
        try {
            ClientInitor.init();
            UploadObject upload = new UploadObject("d:/LICENSE.txt");
            //UploadObject upload = new UploadObject("d:/surfs-lib-2.0.jar");            
            byte[] VHW = upload.upload();

            System.out.println(upload.getVNU());

            DownloadObject obj = new DownloadObject(VHW);
            FileOutputStream out = new FileOutputStream("d:/LICENSE1.txt");
            //FileOutputStream out=new FileOutputStream("d:/wrapper.jar");
            InputStream is = obj.load();
            byte[] bs = new byte[1024];
            int len;
            while ((len = is.read(bs)) != -1) {
                out.write(bs, 0, len);
            }
        } catch (ServiceException se) {
            se.printStackTrace();
            System.out.println(Integer.toHexString(se.getErrorCode()));
        } catch (Exception r) {
            r.printStackTrace();
        }
    }
}
