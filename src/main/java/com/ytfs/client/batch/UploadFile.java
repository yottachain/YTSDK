package com.ytfs.client.batch;

import com.ytfs.client.UploadObject;
import com.ytfs.client.s3.BucketHandler;
import com.ytfs.client.s3.ObjectHandler;
import static com.ytfs.common.ServiceErrorCode.BUCKET_ALREADY_EXISTS;
import com.ytfs.common.ServiceException;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.tanukisoftware.wrapper.WrapperManager;

public class UploadFile extends Thread {
    
    private static final Logger LOG = Logger.getLogger(UploadFile.class);
    static int UPLOAD_SIGN = 0;
    static int UPLOAD_LOOP = 0;
    
    static {
        String num = WrapperManager.getProperties().getProperty("wrapper.batch.uploadSign", "0");
        try {
            UPLOAD_SIGN = Integer.parseInt(num);
        } catch (Exception d) {
        }
        num = WrapperManager.getProperties().getProperty("wrapper.batch.uploadLoopNum", "2");
        try {
            UPLOAD_LOOP = Integer.parseInt(num);
        } catch (Exception d) {
        }
    }
    
    private final String bucketName;
    private final String path;
    
    public UploadFile(String bucketName, String path) {
        this.bucketName = bucketName;
        this.path = path;
    }
    
    @Override
    public void run() {
        MakeFile makeFile = new MakeFile(path, bucketName);
        while (!this.isInterrupted() && UPLOAD_SIGN == 0) {
            try {
                BucketHandler.createBucket(bucketName, new byte[0]);
            } catch (Throwable e) {
                if (e instanceof ServiceException) {
                    ServiceException se = (ServiceException) e;
                    if (se.getErrorCode() == BUCKET_ALREADY_EXISTS) {
                        break;
                    }
                }
                LOG.error("Create Bucket '" + bucketName + "' ERR:" + e.getMessage());
                try {
                    sleep(15000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        int loop = 0;
        while (!this.isInterrupted() && (UPLOAD_LOOP == 0 || loop < UPLOAD_LOOP)) {
            try {
                String uuid = UUID.randomUUID().toString();
                long l = System.currentTimeMillis();
                long len = makeFile.makeFile();
                LOG.info("Make file ok,length " + len / 1024L / 1024L + "M,take times " + (System.currentTimeMillis() - l) + " ms");
                if (UPLOAD_SIGN == 0) {
                    UploadObject obj = new UploadObject(makeFile.getFilePath());
                    obj.upload();
                    ObjectHandler.createObject(bucketName, uuid, obj.getVNU(), new byte[0]);
                } else {
                    UploadFackObject obj = new UploadFackObject(makeFile.getFilePath());
                    obj.upload();
                }
                loop++;
                UploadBooter.total.addAndGet(len);
            } catch (Throwable e) {
                LOG.error("The Bucket '" + bucketName + "' upload file ERR:" + e.getMessage());
                try {
                    sleep(15000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        UploadBooter.delList(this);
    }
    
}
