package com.ytfs.client;

import com.ytfs.service.packet.ServiceException;
import java.io.InputStream;
import sun.misc.IOUtils;

public class UploadSmallTest {

    public static void main(String[] args) {
        String ss="12345678901234567890123456789012345678901234567890";
        ss=ss+"12345678901234567890123456789012345678901234567890";
        ss=ss+"12345678901234567890123456789012345678901234567890";
        ss=ss+"12345678901234567890123456789012345678901234567890";
        try {
            ClientInitor.init();
           // BucketHandler.createBucket("test");
            String[] buckets=BucketHandler.listBucket();
            System.out.println(buckets[0]);
            
            UploadObject upload = new UploadObject(ss.getBytes());
            upload.upload();
            upload.writeMeta("test", "testfile");
            
            DownloadObject obj=new DownloadObject("test", "testfile");
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
