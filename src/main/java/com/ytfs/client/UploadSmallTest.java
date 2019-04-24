package com.ytfs.client;

import com.ytfs.service.packet.ServiceException;
import java.io.InputStream;
import sun.misc.IOUtils;

public class UploadSmallTest {

    public static void main(String[] args) {
        try {
            ClientInitor.init();
            UploadObject upload = new UploadObject("sdsd".getBytes());
            byte[] VHW=upload.upload();
            
            DownloadObject obj=new DownloadObject(VHW);
            InputStream is=obj.load();
            byte[] bs=new byte[1024];
            is.read(bs);
            System.out.println((new String(bs)).trim());
        } catch (ServiceException se) {
            se.printStackTrace();
            System.out.println(Integer.toHexString(se.getErrorCode()));
        } catch (Exception r) {
            r.printStackTrace();
        }
    }
}
