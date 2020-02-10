package com.ytfs.client.batch;

import com.ytfs.client.UploadObject;
import com.ytfs.client.s3.BucketHandler;
import com.ytfs.client.s3.ObjectHandler;
import static com.ytfs.common.ServiceErrorCode.BUCKET_ALREADY_EXISTS;
import com.ytfs.common.ServiceException;
import java.util.UUID;
import org.apache.log4j.Logger;

public class UploadFile extends Thread {

    private static final Logger LOG = Logger.getLogger(UploadFile.class);

    private final String bucketName;
    private final String path;

    public UploadFile(String bucketName, String path) {
        this.bucketName = bucketName;
        this.path = path;
    }

    @Override
    public void run() {
        MakeFile makeFile = new MakeFile(path, bucketName);
        while (!this.isInterrupted()) {
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
        while (!this.isInterrupted()) {
            try {
                String uuid = UUID.randomUUID().toString();
                makeFile.makeFile();
                UploadObject obj = new UploadObject(makeFile.getFilePath());
                obj.upload();
                ObjectHandler.createObject(bucketName, uuid, obj.getVNU(), new byte[0]);
            } catch (Throwable e) {
                LOG.error("The Bucket '" + bucketName + "' upload file ERR:" + e.getMessage());
                try {
                    sleep(15000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

}
